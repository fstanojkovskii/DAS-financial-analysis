import json

import requests
import csv
import os
import argparse
import time
from dotenv import *
from kafka import KafkaProducer


def fetch_daily_data(symbol, api_key):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": api_key,
        "datatype": "json"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "Time Series (Daily)" not in data:
            print(f"Error in response: {data.get('Note', 'Unknown error')}")
            return None

        return data["Time Series (Daily)"]

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        return None


def read_codes(csv_path, test = 3):
    codes = []
    with open(csv_path) as csv_file:
        reader = csv.reader(csv_file)
        next(reader)
        for row in reader:
            if row and len(codes) < test:
                codes.append(row[0].strip())

    return codes


def main():
    load_dotenv()
    api_key = os.getenv("AlphaFree")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )

    csv_path = r"C:\Users\Andreja\Desktop\Das_Financial_analisys\DAS-financial-analysis\services\data-ingestion\src\data\codes.csv" #Menuvaj!!
    codes = read_codes(csv_path)



# parser = argparse.ArgumentParser(description='Fetch stock data')
    # parser.add_argument('symbol', type=str, help='IBM')
    # args = parser.parse_args()
    # Test za 1 variable

    for code in codes:
        print(f"Code: {code}")
        data = fetch_daily_data(codes, api_key)

        count = 0
        for date, values in data.items():
            message = {
                'symbol': code,
                'date': date,
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': int(values['5. volume'])
            }

            producer.send('raw-data', message)
            count += 1

        print(f"Sent {count} daily records for {code}")

        time.sleep(15)  # 15 seconds between symbols

    producer.flush()
    producer.close()
    print("\nData ingestion completed successfully")


if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")
