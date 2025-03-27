from gevent import monkey
monkey.patch_all()

import json
import requests
import os
import time
from dotenv import load_dotenv
from kafka import KafkaProducer
from cassandra.cluster import Cluster
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_cassandra():
    try:
        host = os.getenv('CASSANDRA_HOST', '127.0.0.1')
        cluster = Cluster([host])
        session = cluster.connect('financial_data')
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise

def get_symbols_from_db(session):
    try:
        query = "SELECT symbol FROM symbols"
        rows = session.execute(query)
        return [row.symbol for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch symbols from database: {e}")
        raise

def check_symbol_data_exists(session, symbol):
    try:
        query = "SELECT COUNT(*) FROM stock_prices WHERE symbol = %s"
        rows = session.execute(query, [symbol])
        count = rows[0].count
        return count > 0
    except Exception as e:
        logger.error(f"Failed to check data existence for {symbol}: {e}")
        return False

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
            logger.error(f"Error in response for {symbol}: {data.get('Note', 'Unknown error')}")
            return None

        return data["Time Series (Daily)"]

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {symbol}: {str(e)}")
        return None

def main():
    load_dotenv()
    api_key = os.getenv("AlphaFree")

    if not api_key:
        logger.error("API key not found in environment variables")
        return

    try:
        cluster, session = connect_to_cassandra()
        
        symbols = get_symbols_from_db(session)
        logger.info(f"Found {len(symbols)} symbols in database")
        
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        )

        for symbol in symbols:
            if check_symbol_data_exists(session, symbol):
                logger.info(f"Skipping {symbol} - data already exists")
                continue

            logger.info(f"Fetching data for: {symbol}")
            data = fetch_daily_data(symbol, api_key)

            if not data:
                continue

            count = 0
            for date, values in data.items():
                message = {
                    'symbol': symbol,
                    'date': date,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume'])
                }
                producer.send('raw-data', message)
                count += 1

            logger.info(f"Sent {count} records for {symbol}")
            time.sleep(15)

        producer.flush()
        producer.close()
        cluster.shutdown()
        
        logger.info("Data ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"Main process failed: {e}")
        raise

if __name__ == "__main__":
    start_time = time.time()
    main()
    logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")