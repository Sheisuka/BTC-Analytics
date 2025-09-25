import requests
import logging
import json
import sys
import csv
import pathlib
from datetime import datetime


FNG_API_URL = 'https://api.alternative.me/fng'
FNG_OUTPUT_PATH = pathlib.Path('data/raw/fng18_25.csv')
RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/alternative.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


def fetch_fng(limit=0, date_format='world', retries_count=RETRIES):
    resp_format = 'json'
    url = f'{FNG_API_URL}/?limit={limit}&format={resp_format}&date_format={date_format}'

    try:
        response = requests.get(url)
        if response.status_code == 429 and retries_count:
            return fetch_fng(retries_count=retries_count-1)
        if response.status_code != 200:
            logging.error(f'Status code {response.status_code} for {url}')
            return None
        try:
            return response.json()
        except Exception as e:
            logging.error(f'Error decoding json {url} | exception {e}')
            return None 

    except Exception as e:
        logging.error(f'Error fetching {url} | exception {e}')
        return None
    

def write_fng(path=FNG_OUTPUT_PATH):
    response = fetch_fng()
    if response is None:
        logging.error(f'Failed to fetch fng data')
        return

    records = response['data']
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['value', 'classification', 'timestamp'])
        for record in records:
            value, value_class, ts = record['value'], record['value_classification'], record['timestamp']
            writer.writerow([value, value_class, ts])
    
    logging.info('Wrote %d rows into %s', len(records), path)

if __name__ == '__main__':
    write_fng()