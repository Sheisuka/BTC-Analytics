import os
import logging
import aiohttp
import asyncio
import time
from datetime import datetime

from dotenv import load_dotenv


load_dotenv(".env")

COINDESK_API_KEY = os.getenv('COINDESK_API_KEY')
COINDESK_OHCLV_URL = 'https://data-api.coindesk.com/index/cc/v1/historical/days'

asyncio.run(main())


async def fetch(session, semaphore, to_ts, limit=2000, retries_left=3):
    params = {
        'market': 'cadli',
        'instrument': 'BTC-USD',
        'groups': 'OHLC,VOLUME',
        'response_format': 'CSV',
        'limit': limit,
        'to_ts': to_ts,
        'api_key': COINDESK_API_KEY
    }
    headers = { 'Content-type': 'application/json;charset=UTF-8' }

    async with semaphore:
        try:
            async with session.get(COINDESK_OHCLV_URL, params=params, headers=headers) as response:
                if response.status == 429 and retries_left:
                    time.sleep(5)
                    return await fetch(session, semaphore, to_ts, limit, retries_left=retries_left-1)

                if response.status != 200:
                    logging.info(f"Status code {response.status} for  URL {COINDESK_OHCLV_URL}  | to_ts {to_ts}")
                    return None
                
                data = await response.text()
                return data

        except Exception as e:
            logging.error(f"Error fetching URL {COINDESK_OHCLV_URL} | to_ts {to_ts} | exception {e}")
            return None
                    


async def fetch_hystorical_ohlcv(session, semaphore):
    tasks = []


    start_date = int(datetime.strptime('01-01-2013', '%d-%m-%Y').timestamp())
    end_date = int(datetime.strptime('30-05-2025', '%d-%m-%Y').timestamp())
    step = 86400
    limit = 2000

    for from_ts in range (start_date, end_date + step*limit, step * limit):
        to_ts = min(from_ts + step, end_date)
        task = asyncio.create_task(fetch(session, semaphore, to_ts, limit))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks)

    return results

async def get_ohcl():
    semaphore = asyncio.Semaphore(1)
    async with aiohttp.ClientSession() as session:
        ohcl_data = await fetch_hystorical_ohlcv(session, semaphore)
    
    header = ohcl_data[0].splitlines()[0]

    with open("data/raw/btc_ohlc13_25.csv", "w", newline='', encoding="utf-8") as f:
        f.write(header + "\n")
        for chunk in ohcl_data:
            if chunk is None:
                continue
            for line in chunk.splitlines()[1:]:
                f.write(line + "\n")


async def main():
    await get_ohcl()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("src/etl/btc_ohlc13_25.log"),
        logging.StreamHandler()
    ]
)
