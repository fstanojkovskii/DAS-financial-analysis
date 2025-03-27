from gevent import monkey
monkey.patch_all()  # Patch standard library functions

import requests
import os
from dotenv import load_dotenv
from cassandra.cluster import Cluster
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_cassandra():
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect('financial_data')
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise

def fetch_top_companies(api_key):
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        "limit": 500,
        "sort": "marketCap",
        "sortDir": "desc",
        "apikey": api_key
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        companies = response.json()


        for company in companies:
            company["marketCap"] = float(company.get("marketCap", 0))

        sorted_companies = sorted(
            companies,
            key=lambda x: x["marketCap"],
            reverse=True
        )
        #Bez Indeksi
        industries = {
            "Asset Management",
            "Asset Management - Leveraged",
            "Banks - Diversified",
            "Financial - Data & Stock Exchanges",
            "Drug Manufacturers - General",
            "Healthcare",
            "Asset Management - Income"
        }
        indexes = {
            "index", "vanguard", "fund", "etf", "etn", "idx", "trust",
            "financial", "bank", "asset", "management", "exchange",
        }
        duplicates = {
            "GOOG",
            "BRK-B"
        }
        filtered_companies = [
            company for company in sorted_companies
            if company.get("sector") and company.get("industry") and company.get("symbol")
               and company["industry"] not in industries
               and not any(keyword in company["companyName"].lower() for keyword in indexes)
               and company["symbol"] not in duplicates
               and company["sector"] not in industries


        ]
        final_companies = filtered_companies[:100]

        return final_companies
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        return None

def store_symbols_in_db(session, companies):
    try:
        # Prepare the insert statement
        insert_query = "INSERT INTO symbols (symbol) VALUES (%s)"
        
        # Insert each symbol
        for company in companies:
            symbol = company.get("symbol")
            if symbol:
                session.execute(insert_query, (symbol,))
        
        logger.info(f"Successfully stored {len(companies)} symbols in the database")
    except Exception as e:
        logger.error(f"Failed to store symbols in database: {e}")
        raise

def main():
    load_dotenv()
    api_key = os.getenv("FMPFree")

    companies = fetch_top_companies(api_key)

    if companies:
        try:
            cluster, session = connect_to_cassandra()
            store_symbols_in_db(session, companies)
            cluster.shutdown()
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
    else:
        logger.error("No companies data to store")


if __name__ == "__main__":
    main()