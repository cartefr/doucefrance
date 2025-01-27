import pandas as pd
import requests
import os
from dotenv import load_dotenv
import json

# Charger les variables d'environnement
load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
TABLE_NAME = 'faits_divers'

headers = {
    'apikey': SUPABASE_SERVICE_ROLE_KEY,
    'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

def load_csv_to_dataframe(csv_path):
    df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
    
    # Nettoyer les données : supprimer les guillemets autour des chaînes de caractères
    df = df.apply(lambda x: x.str.strip('"') if x.dtype == "object" else x)
    
    # Convertir 'latitude' et 'longitude' en numériques
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    # Remplacer inf par NaN
    df['latitude'] = df['latitude'].replace([float('inf'), float('-inf')], pd.NA)
    df['longitude'] = df['longitude'].replace([float('inf'), float('-inf')], pd.NA)
    
    # Supprimer lignes invalides
    df.dropna(subset=['latitude','longitude'], inplace=True)
    
    # Convertir 'article_label' en liste
    df['article_label'] = df['article_label'].apply(lambda x: [lbl.strip() for lbl in x.split(',')] if pd.notnull(x) else [])
    
    # NaN => None
    if 'lien_source' in df.columns:
        df['lien_source'] = df['lien_source'].astype(object).where(pd.notnull(df['lien_source']), None)

    df = df.where(pd.notnull(df), None)
    return df

def delete_existing_data():
    # (Optionnel) Supprime toutes les lignes existantes
    delete_response = requests.delete(
        f'{SUPABASE_URL}/rest/v1/{TABLE_NAME}',
        headers=headers,
        params={'id': 'gt.0'}  # 'id > 0'
    )
    if delete_response.status_code in [200, 204]:
        print('Données précédentes supprimées avec succès.')
    else:
        print(f'Erreur suppression : {delete_response.text}')
        raise Exception("Suppression échouée.")

def insert_data(df):
    batch_size = 1000
    records = df.to_dict(orient='records')
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            json.dumps(batch)
        except Exception as e:
            print(f"Erreur JSON batch {i // batch_size + 1}: {e}")
            raise
        
        response = requests.post(
            f'{SUPABASE_URL}/rest/v1/{TABLE_NAME}',
            headers=headers,
            json=batch
        )
        if response.status_code in [200,201]:
            print(f"Batch {i // batch_size + 1} insérée.")
        else:
            raise Exception(f"Erreur insertion batch {i // batch_size + 1} : {response.text}")

def main():
    csv_path = 'bdd.csv'
    df = load_csv_to_dataframe(csv_path)
    
    # Log
    print("=== Aperçu DataFrame ===")
    print(df.head())

    # Supprimer données existantes (optionnel)
    # delete_existing_data()

    insert_data(df)

if __name__=='__main__':
    main()
