import os
import sys
import zipfile
from datetime import datetime, timedelta

import requests

whitelist = [
    "BTCUSDT",
    "ETHUSDT",
    "ADAUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "LINKUSDT",
    "DOTUSDT",
    "LTCUSDT",
    "TWTUSDT",
    "DOGEUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "DEFIUSDT",
    "TRBUSDT"
]


def get_start_date():
    # Returns 2020-01-01
    return datetime(2020, 1, 1)


def get_end_date():
    # Returns previous month first date
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.replace(day=1)


def generate_binance_urls(symbol, timeframe, start_date):
    current_date = start_date
    end_date = get_end_date()
    urls = []

    while current_date <= end_date:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{year}-{month}.zip"
        urls.append(url)
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)

    return urls


def download_file(url, directory):
    local_filename = url.split("/")[-1]
    full_path = os.path.join(directory, local_filename)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(full_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return full_path


def unpack_asset(zip_file_path, symbol, timeframe):
    try:
        # Unzips zip file content to ./binance/{symbol}_{timeframe}_futures
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(f"./binance/{symbol}_{timeframe}_futures")

        # Deletes zip file
        os.remove(zip_file_path)

        print(f"Unpacked {zip_file_path}")
    except Exception as e:
        print(f"Failed to unpack {zip_file_path}: {e}")


def get_symbol_start_date(symbol, timeframe):
    # Finds the last symbol date in ./binance/{symbol}_{timeframe}_futures
    # (filename example: ./binance/BTCUSDT_4h_futures/BTCUSDT-4h-2023-08.csv)
    # or returns get_start_date() + 1 month if the folder does not exist
    folder = f"./binance/{symbol}_{timeframe}_futures"
    if os.path.exists(folder):
        files = os.listdir(folder)
        files.sort(reverse=True)
        last_file = files[0]
        year = last_file.split("-")[2]
        month = last_file.split("-")[3].split(".")[0]
        start_date = datetime(int(year), int(month), 1)
        return start_date
    else:
        return get_start_date()


def process_symbol(symbol, timeframe):
    print(f"Processing {symbol} {timeframe}")

    start_date = get_symbol_start_date(symbol, timeframe)
    print(f"Start date: {start_date.date()}, end date: {get_end_date().date()}")

    if start_date.date() >= get_end_date().date():
        print(f"Already up to date")
        return

    urls = generate_binance_urls(symbol, timeframe, start_date)
    download_directory = f"./downloads"

    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    for url in urls:
        print(f"Downloading {url}")
        try:
            file_path = download_file(url, download_directory)
            print(f"Downloaded to {file_path}")

            unpack_asset(file_path, symbol, timeframe)
        except Exception as e:
            print(f"Failed to download {url}: {e}")


if len(sys.argv) < 2:
    print("Usage: python script.py <timeframe>")
else:
    timeframe = sys.argv[1]

    for symbol in whitelist:
        process_symbol(symbol, timeframe)
