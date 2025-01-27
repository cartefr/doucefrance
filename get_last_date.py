#!/usr/bin/env python3
import os
import requests
import json
from dotenv import load_dotenv

def main():
    load_dotenv()
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    TABLE_NAME = 'faits_divers'

    headers = {
        'apikey': SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}'
    }

    params = {
        'select': 'date',
        'order': 'date.desc',
        'limit': '1'
    }
    resp = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}", headers=headers, params=params)
    if resp.status_code == 200:
        data = resp.json()
        if data:
            last_date = data[0]['date']
            print(last_date)
        else:
            print("2022-01-01")  # fallback
    else:
        print("2022-01-01")  # fallback

if __name__=='__main__':
    main()
