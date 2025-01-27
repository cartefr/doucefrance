#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
import unidecode
from tqdm import tqdm
import re
import argparse
import os

# ================== Optionnel : Récupérer dernière date depuis Supabase ==================
def get_last_date_from_supabase():
    """
    Exemple minimal pour récupérer la date max depuis Supabase.
    Requiert un usage de requests sur la table 'faits_divers'.
    """
    import requests
    import os
    from dotenv import load_dotenv
    
    load_dotenv()  # charge SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    TABLE_NAME = 'faits_divers'

    headers = {
        'apikey': SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    }

    # On tente de récupérer la date max
    # Filtre : on sélectionne la colonne date, triée desc, limit=1
    params = {
        'select': 'date',
        'order': 'date.desc',
        'limit': '1'
    }
    resp = requests.get(f'{SUPABASE_URL}/rest/v1/{TABLE_NAME}', headers=headers, params=params)
    if resp.status_code == 200:
        data = resp.json()
        if len(data) > 0:
            return data[0]['date']  # ex "2025-01-05"
        else:
            return None
    else:
        print(f"[WARN] Impossible de récupérer la dernière date (HTTP {resp.status_code}) => {resp.text}")
        return None

def transform_label_for_dict(label: str) -> str:
    label_norm = unidecode.unidecode(label).lower()
    label_norm = label_norm.replace('-', ' ')
    label_norm = label_norm.replace("'", ' ')
    label_norm = label_norm.replace('sainte', 'ste')
    label_norm = label_norm.replace('saint', 'st')
    label_norm = label_norm.strip()
    return label_norm

def load_cities(cities_csv_path):
    cities_dict_dept = {}
    cities_dict_nodept = {}
    with open(cities_csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            label_original = row['label'].strip()
            lat = row['latitude'].strip()
            lon = row['longitude'].strip()
            dept = row['department_number'].strip()
            label_norm = transform_label_for_dict(label_original)

            cities_dict_dept[(label_norm, dept)] = (label_original, lat, lon)
            if label_norm not in cities_dict_nodept:
                cities_dict_nodept[label_norm] = []
            cities_dict_nodept[label_norm].append((label_original, lat, lon, dept))

    return cities_dict_nodept, cities_dict_dept

def load_popular_cities_csv(popular_csv_path):
    pop_dict = {}
    with open(popular_csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city_raw = row['city'].strip()
            code = row['code'].strip()
            city_norm = transform_label_for_dict(city_raw)
            pop_dict[city_norm] = code
    return pop_dict

def find_city_in_title(title, popular_cities_dict, cities_dict_nodept, cities_dict_dept):
    title_norm = unidecode.unidecode(title).lower()

    # 1) Pattern (dept)
    match_dept = re.search(r'\((\d{1,3})\)', title_norm)
    if match_dept:
        dept_num = match_dept.group(1)
        city_part = title[:match_dept.start()]
        city_part_norm = transform_label_for_dict(city_part)

        tokens = city_part_norm.split()
        for size in range(min(5,len(tokens)),0,-1):
            chunk = tokens[-size:]
            chunk_join = ' '.join(chunk)
            chunk_norm = transform_label_for_dict(chunk_join)
            if (chunk_norm, dept_num) in cities_dict_dept:
                label_original, lat, lon = cities_dict_dept[(chunk_norm, dept_num)]
                return (label_original, lat, lon, dept_num)

    # 2) fallback => popular cities
    for pop_city_norm, dept_code in popular_cities_dict.items():
        if pop_city_norm in title_norm:
            if (pop_city_norm, dept_code) in cities_dict_dept:
                label_original, lat, lon = cities_dict_dept[(pop_city_norm, dept_code)]
                return (label_original, lat, lon, dept_code)

    return None

def fetch_article_details(article_url):
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
            return ("","","")
    except Exception as e:
        print(f"[DEBUG] Erreur fetch_article_details : {e}")
        return ("","","")

def scrape_fdesouche(start_date, end_date,
                     popular_cities_dict,
                     cities_dict_nodept,
                     cities_dict_dept,
                     output_csv_path):
    from datetime import datetime, timedelta
    import csv
    import os

    fieldnames = [
        'id','date','ville','latitude','longitude',
        'titre','contenu','lien_fdesouche','lien_source','article_label','code_dpt'
    ]
    date_format = "%Y-%m-%d"
    current_date = datetime.strptime(start_date, date_format)
    stop_date = datetime.strptime(end_date, date_format)

    mode = 'a'
    if not os.path.isfile(output_csv_path):
        mode = 'w'

    compteur = 1
    if os.path.isfile(output_csv_path):
        # Calculer l'ID max déjà utilisé
        with open(output_csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            max_id = 0
            for row in reader:
                try:
                    row_id = int(row['id'].strip('"'))
                    if row_id > max_id:
                        max_id = row_id
                except:
                    pass
            compteur = max_id + 1

    total_days = (stop_date - current_date).days + 1

    from tqdm import tqdm
    with open(output_csv_path, mode=mode, encoding='utf-8', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_ALL)
        if mode=='w':
            writer.writeheader()

        with tqdm(total=total_days, desc="Scraping days") as pbar:
            while current_date <= stop_date:
                day_str = current_date.strftime(date_format)
                page_index = 1
                found_any_article = False

                while True:
                    url_day = current_date.strftime(f"https://www.fdesouche.com/%Y/%m/%d/page/{page_index}/")
                    try:
                        resp = requests.get(url_day, timeout=10)
                        if resp.status_code == 200:
                            soup = BeautifulSoup(resp.text, "html.parser")
                            article_blocks = soup.select("article")
                            if not article_blocks:
                                break
                            found_any_article = True
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
                                        writer.writerow({
                                            'id': f'"{compteur}"',
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
                                        compteur+=1
                            page_index += 1
                            time.sleep(0.5)
                        else:
                            break
                    except Exception as e:
                        print(f"[DEBUG] Erreur sur {url_day} : {e}")
                        break
                current_date += timedelta(days=1)
                pbar.update(1)

def main():
    parser = argparse.ArgumentParser(description="Scraping Fdesouche (articles, villes, etc.)")
    parser.add_argument('--start', type=str, help="Date de début (YYYY-MM-DD)")
    parser.add_argument('--end', type=str, help="Date de fin (YYYY-MM-DD)")
    parser.add_argument('--cities', type=str, default="cities.csv", help="Chemin vers cities.csv")
    parser.add_argument('--popular-cities', type=str, default="popular_cities.csv", help="CSV listant les villes populaires (city,code)")
    parser.add_argument('--out', type=str, default="bdd.csv", help="Nom du CSV de sortie")
    parser.add_argument('--auto', action='store_true', help="Tente de recup la date max depuis Supabase")

    args = parser.parse_args()

    # Charger dicts
    cities_dict_nodept, cities_dict_dept = load_cities(args.cities)
    popular_cities_dict = load_popular_cities_csv(args.popular_cities)

    # Logique auto
    if args.auto:
        last_date = get_last_date_from_supabase()
        if last_date:
            start_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            start = start_dt.strftime("%Y-%m-%d")
            end = datetime.today().strftime("%Y-%m-%d")
            print(f"[INFO] Start={start}, End={end}")
            scrape_fdesouche(start, end, popular_cities_dict, cities_dict_nodept, cities_dict_dept, args.out)
        else:
            print("[WARN] Aucune date trouvée, on prend 1er janv 2022.")
            scrape_fdesouche("2022-01-01", datetime.today().strftime("%Y-%m-%d"), popular_cities_dict, cities_dict_nodept, cities_dict_dept, args.out)
    else:
        # classiquement
        start = args.start if args.start else "2022-01-01"
        end = args.end if args.end else datetime.today().strftime("%Y-%m-%d")
        scrape_fdesouche(start, end, popular_cities_dict, cities_dict_nodept, cities_dict_dept, args.out)

if __name__ == "__main__":
    main()
