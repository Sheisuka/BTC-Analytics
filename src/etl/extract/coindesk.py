import os
import logging
import aiohttp
import asyncio
import time
import pathlib
import csv
import sys
from datetime import datetime

from dotenv import load_dotenv


load_dotenv('.env')

API_KEY = os.getenv('COINDESK_API_KEY')
OHLC_API_URL = 'https://data-api.coindesk.com/index/cc/v1/historical/days'
OHLC_OUTPUT_PATH = pathlib.Path('data/raw/btc_ohlc13_25.csv')
RETRIES = 3
RETRY_BACKOFF = 5
WORKERS = 5

if not API_KEY:
    raise RuntimeError('coindesk api key is missing')

semaphore = asyncio.Semaphore(WORKERS)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/coindesk.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


async def fetch_url(session, url, params=None, headers=None, retries_left=RETRIES):
    if params is None: params = {}
    if headers is None: headers = {}
    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 429 and retries_left:
                    await asyncio.sleep(RETRY_BACKOFF)
                    return await fetch_url(session, url, params, headers, retries_left=retries_left-1)

                if response.status != 200:
                    logging.info(f'Status code {response.status} for {url} | params {params}')
                    return None

                return await response.text()

        except Exception as e:
            logging.error(f'Error fetching {url} | exception {e} | params {params}')
            return None


def build_to_ts_list(start_ts, end_ts, step, limit):
    first_to = start_ts + (limit - 1) * step
    if first_to > end_ts:
        return [end_ts]
    to_list = list(range(first_to, end_ts + 1, limit * step))
    if to_list[-1] != end_ts:
        to_list.append(end_ts)
    return to_list


async def fetch_daily_ohlcv(start_ts, end_ts):
    step = 86400
    limit = 2000
    url = OHLC_API_URL
    tasks = []

    to_list = build_to_ts_list(start_ts, end_ts, step, limit)

    async with aiohttp.ClientSession() as session:
        for to_ts in to_list:
            params = {
                'market': 'cadli',
                'instrument': 'BTC-USD',
                'groups': 'OHLC,VOLUME',
                'response_format': 'CSV',
                'limit': limit,
                'to_ts': to_ts,
                'api_key': API_KEY
            }
            tasks.append(asyncio.create_task(fetch_url(session, url, params)))
        results = await asyncio.gather(*tasks)

    return results


async def write_ohcl(start_ts, end_ts, path=OHLC_OUTPUT_PATH):
    response = await fetch_daily_ohlcv(start_ts, end_ts)
    if response is None:
        logging.error(f'Failed to fetch ohcl data')
        return

    rows = 0
    reader = csv.reader(response[0].splitlines())
    header = next(reader)
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        
        for chunk in response:
            if chunk is None:
                continue
            for row in csv.reader(chunk.splitlines()[1:]):
                writer.writerow(row)
                rows += 1
    
    logging.info('Wrote %d rows into %s', rows, path)


async def main():
    start_ts = int(datetime.strptime('01-01-2013', '%d-%m-%Y').timestamp())
    end_ts = int(datetime.strptime('30-05-2025', '%d-%m-%Y').timestamp())

    await write_ohcl(start_ts, end_ts)

asyncio.run(main())