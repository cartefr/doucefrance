#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import unidecode
import re
import argparse
import os
import logging
from supabase import create_client, Client
from dotenv import load_dotenv
import time

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,  # Changez en DEBUG pour plus de détails
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("Les variables d'environnement SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY doivent être définies.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

TABLE_NAME = 'faits_divers'

def get_max_id():
    """
    Récupère le maximum de l'ID actuel dans la table 'faits_divers' de Supabase.
    """
    try:
        response = supabase.table(TABLE_NAME).select('id').order('id', desc=True).limit(1).execute()
        data = response.data
        print("data est", data)
        if data:
            try:
                max_id = int(data[0]['id'])
                logging.info(f"Max ID actuel dans Supabase : {max_id}")
                return max_id
            except ValueError:
                logging.warning(f"L'ID récupéré n'est pas un entier : {data[0]['id']}")
                return 0
        else:
            logging.info("Aucun enregistrement trouvé dans Supabase. Commencer à partir de 1.")
            return 0
    except Exception as e:
        logging.error(f"Exception lors de la récupération du max ID : {e}")
        return 0

def get_existing_links_for_day(day_str):
    """
    Récupère tous les 'lien_fdesouche' des articles pour une date donnée depuis Supabase.
    """
    try:
        response = supabase.table(TABLE_NAME).select('lien_fdesouche').eq('date', day_str).execute()
        existing_links = set()
        for item in response.data:
            if 'lien_fdesouche' in item and item['lien_fdesouche']:
                existing_links.add(item['lien_fdesouche'])
        logging.info(f"{len(existing_links)} liens existants récupérés pour la date {day_str}.")
        return existing_links
    except Exception as e:
        logging.error(f"Exception lors de la récupération des liens existants : {e}")
        return set()

def transform_label_for_dict(label: str) -> str:
    """
    Normalise un nom de ville ou label pour utilisation comme clé.
    """
    label_norm = unidecode.unidecode(label).lower()
    label_norm = label_norm.replace('-', ' ')
    label_norm = label_norm.replace("'", ' ')
    label_norm = label_norm.replace('sainte', 'ste')
    label_norm = label_norm.replace('saint', 'st')
    label_norm = label_norm.strip()
    return label_norm

def load_cities(cities_csv_path):
    """
    Lit cities.csv et retourne des dictionnaires pour la recherche de villes.
    """
    cities_dict_dept = {}
    cities_dict_nodept = {}
    with open(cities_csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            label_original = row['label'].strip()
            if row['latitude'] != '' and row['longitude'] != '':
                try:
                    lat = float(row['latitude'].strip())
                    lon = float(row['longitude'].strip())
                except ValueError:
                    logging.warning(f"Latitude ou longitude non valide pour la ville {label_original}. Skipping.")
                    continue
                dept = row['department_number'].strip()
                label_norm = transform_label_for_dict(label_original)

                cities_dict_dept[(label_norm, dept)] = (label_original, lat, lon)
                if label_norm not in cities_dict_nodept:
                    cities_dict_nodept[label_norm] = []
                cities_dict_nodept[label_norm].append((label_original, lat, lon, dept))
    logging.info(f"Chargement des villes depuis {cities_csv_path} terminé.")
    return cities_dict_nodept, cities_dict_dept

def load_popular_cities_csv(popular_csv_path):
    """
    Lit popular_cities.csv et retourne un dictionnaire de villes populaires.
    """
    pop_dict = {}
    with open(popular_csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city_raw = row['city'].strip()
            code = row['code'].strip()
            city_norm = transform_label_for_dict(city_raw)
            pop_dict[city_norm] = code
    logging.info(f"Chargement des villes populaires depuis {popular_csv_path} terminé.")
    return pop_dict

def find_city_in_title(title, popular_cities_dict, cities_dict_nodept, cities_dict_dept):
    """
    Trouve la ville dans le titre de l'article.
    """
    title_norm = unidecode.unidecode(title).lower()
    logging.debug(f"Titre normalisé pour la recherche de ville : '{title_norm}'")

    # 1) Pattern (dept) avec gestion des caractères de ponctuation après la parenthèse
    match_dept = re.search(r'\((\d{1,3})\)', title_norm)
    if match_dept:
        dept_num = match_dept.group(1)
        # Extraire la partie du titre avant la parenthèse
        city_part = title[:match_dept.start()]
        # Supprimer les espaces et la ponctuation finale
        city_part = city_part.strip().rstrip('.!?')  # Ajout de rstrip pour enlever la ponctuation
        city_part_norm = transform_label_for_dict(city_part)
        logging.debug(f"Partie de la ville extraite : '{city_part_norm}', Département : '{dept_num}'")

        tokens = city_part_norm.split()
        # Parcourir les tokens de la ville de la plus longue sous-chaîne à la plus courte
        for size in range(min(5, len(tokens)), 0, -1):
            chunk = tokens[-size:]
            chunk_join = ' '.join(chunk)
            chunk_norm = transform_label_for_dict(chunk_join)
            logging.debug(f"Vérification du chunk : '{chunk_norm}' avec Département : '{dept_num}'")
            if (chunk_norm, dept_num) in cities_dict_dept:
                label_original, lat, lon = cities_dict_dept[(chunk_norm, dept_num)]
                logging.info(f"Ville reconnue : {label_original} (Département {dept_num})")
                return (label_original, lat, lon, dept_num)

        # Si aucune correspondance exacte, essayer une correspondance partielle
        for (city_norm, dept), (label_original, lat, lon) in cities_dict_dept.items():
            if dept == dept_num and city_norm.startswith(city_part_norm):
                logging.info(f"Ville reconnue (partielle) : {label_original} (Département {dept_num})")
                return (label_original, lat, lon, dept_num)

    # 2) Fallback => villes populaires
    for pop_city_norm, dept_code in popular_cities_dict.items():
        if pop_city_norm in title_norm:
            # Prioriser les correspondances exactes dans les villes populaires
            if (pop_city_norm, dept_code) in cities_dict_dept:
                label_original, lat, lon = cities_dict_dept[(pop_city_norm, dept_code)]
                logging.info(f"Ville reconnue via ville populaire : {label_original} (Département {dept_code})")
                return (label_original, lat, lon, dept_code)
            # Sinon, essayer des correspondances partielles
            else:
                for (city_norm, dept), (label_original, lat, lon) in cities_dict_dept.items():
                    if dept == dept_code and city_norm.startswith(pop_city_norm):
                        logging.info(f"Ville reconnue via ville populaire (partielle) : {label_original} (Département {dept_code})")
                        return (label_original, lat, lon, dept_code)

    logging.warning(f"Aucune ville reconnue dans le titre : '{title}'")
    return None

def fetch_article_details(article_url):
    """
    Récupère le contenu complet de l'article, le lien source et les labels.
    """
    try:
        resp = requests.get(article_url, timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            content_block = soup.select_one("div.entry-content")
            contenu_complet = content_block.get_text(separator='\n', strip=True) if content_block else ""
            contenu_complet = contenu_complet.replace('"',' ')
            lien_source = ""
            possible_links = soup.select("div.entry-content a")
            for link in possible_links:
                href = link.get('href','')
                link_text = link.get_text(strip=True).lower()
                if "source" in link_text or "via" in link_text:
                    lien_source = href
                    break
            article_labels = []
            cat_div = soup.select_one("div.entry-category")
            if cat_div:
                cat_links = cat_div.select("a")
                for cat_link in cat_links:
                    lbl_txt = cat_link.get_text(strip=True)
                    if lbl_txt:
                        article_labels.append(lbl_txt)
            article_label = ",".join(article_labels)
            return (contenu_complet, lien_source, article_label)
        else:
            logging.warning(f"Échec de la récupération de {article_url} (HTTP {resp.status_code})")
            return ("","","")
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des détails de l'article {article_url} : {e}")
        return ("","","")

def scrape_today(popular_cities_dict, cities_dict_nodept, cities_dict_dept):
    """
    Scrape les articles de la date d'aujourd'hui.
    """
    today = datetime.today().strftime("%Y-%m-%d")
    date_format = "%Y-%m-%d"
    current_date = datetime.strptime(today, date_format)
    day_str = current_date.strftime(date_format)
    page_index = 1
    articles = []

    while True:
        url_day = current_date.strftime(f"https://www.fdesouche.com/%Y/%m/%d/page/{page_index}/")
        try:
            resp = requests.get(url_day, timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                article_blocks = soup.select("article")
                if not article_blocks:
                    logging.info(f"Aucun article trouvé sur {url_day}.")
                    break
                for block in article_blocks:
                    title_tag = block.select_one("h2.entry-title a")
                    if title_tag:
                        titre = title_tag.get_text(strip=True)
                        lien_fdesouche = title_tag.get('href','')
                        contenu, lien_source, article_label = fetch_article_details(lien_fdesouche)
                        titre = titre.replace('\n',' ').replace('\r',' ').strip()
                        contenu = contenu.replace('\n',' ').replace('\r',' ').strip()

                        found_city = find_city_in_title(
                            titre, popular_cities_dict, cities_dict_nodept, cities_dict_dept
                        )
                        if found_city:
                            city_label, lat, lon, dept = found_city
                            articles.append({
                                'date': day_str,
                                'ville': city_label,
                                'latitude': lat,
                                'longitude': lon,
                                'titre': titre,
                                'contenu': contenu,
                                'lien_fdesouche': lien_fdesouche,
                                'lien_source': lien_source,
                                'article_label': article_label,
                                'code_dpt': dept
                            })
                page_index +=1
                time.sleep(0.5)  # Respecter les délais pour éviter d'être bloqué
            else:
                logging.warning(f"HTTP {resp.status_code} pour l'URL {url_day}, arrêt de la pagination.")
                break
        except Exception as e:
            logging.error(f"Erreur lors du scraping de {url_day} : {e}")
            break

    logging.info(f"{len(articles)} articles trouvés pour la date {day_str}.")
    return articles

def insert_articles(articles, start_id, existing_links):
    """
    Insère directement les articles dans Supabase avec des IDs séquentiels,
    en évitant les doublons basés sur 'lien_fdesouche'.
    """
    if not articles:
        logging.info("Aucun article à insérer.")
        return

    records_to_insert = []
    compteur = start_id

    for article in articles:
        if article['lien_fdesouche'] in existing_links:
            logging.info(f"Article déjà présent, skipping: {article['lien_fdesouche']}")
            continue
        record = {
            'id': compteur,
            'date': article['date'],
            'ville': article['ville'],
            'latitude': article['latitude'],
            'longitude': article['longitude'],
            'titre': article['titre'],
            'contenu': article['contenu'],
            'lien_fdesouche': article['lien_fdesouche'],
            'lien_source': article['lien_source'],
            'article_label': article['article_label'].split(',') if article['article_label'] else [],
            'code_dpt': article['code_dpt']
        }
        records_to_insert.append(record)
        compteur +=1

    if not records_to_insert:
        logging.info("Aucun nouvel article à insérer après vérification des doublons.")
        return

   
    supabase.table(TABLE_NAME).insert(records_to_insert).execute()
    
def main():
    parser = argparse.ArgumentParser(description="Scraping Fdesouche et insertion dans Supabase.")
    parser.add_argument('--cities', type=str, default="cities.csv", help="Chemin vers cities.csv")
    parser.add_argument('--popular-cities', type=str, default="popular_cities.csv", help="Chemin vers popular_cities.csv")
    args = parser.parse_args()

    # Charger les dictionnaires de villes
    cities_dict_nodept, cities_dict_dept = load_cities(args.cities)
    popular_cities_dict = load_popular_cities_csv(args.popular_cities)

    # Scraper les articles de la date d'aujourd'hui
    articles = scrape_today(popular_cities_dict, cities_dict_nodept, cities_dict_dept)

    if not articles:
        logging.info("Aucun nouvel article à insérer pour aujourd'hui.")
        return

    # Récupérer le max ID actuel dans Supabase
    max_id = get_max_id()

    # Récupérer les liens existants pour la date d'aujourd'hui
    day_str = articles[0]['date'] if articles else datetime.today().strftime("%Y-%m-%d")
    existing_links = get_existing_links_for_day(day_str)

    # Insérer les nouveaux articles avec des IDs séquentiels, en évitant les doublons
    insert_articles(articles, max_id +1, existing_links)

if __name__ == "__main__":
    main()
