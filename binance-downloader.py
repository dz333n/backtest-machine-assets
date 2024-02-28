import sys
import requests
from datetime import datetime, timedelta
import os

def generate_binance_url(symbol, timeframe):
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 1, 1)
    current_date = start_date
    urls = []

    while current_date < end_date:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{year}-{month}.zip"
        urls.append(url)
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)

    return urls

def download_file(url, directory):
    local_filename = url.split('/')[-1]
    full_path = os.path.join(directory, local_filename)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(full_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

if len(sys.argv) < 3:
    print("Usage: python script.py <symbol> <timeframe>")
else:
    symbol = sys.argv[1]
    timeframe = sys.argv[2]

    urls = generate_binance_url(symbol, timeframe)
    download_directory = "./downloads"

    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    for url in urls:
        print(f"Downloading {url}")
        try:
            download_file(url, download_directory)
            print(f"Downloaded to {download_directory}")
        except Exception as e:
            print(f"Failed to download {url}: {e}")
