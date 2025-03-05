import requests
import csv
import os
from dotenv import load_dotenv


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
        print(f"API request failed: {str(e)}")
        return None

def main():
    load_dotenv()
    api_key = os.getenv("FMPFree")

    companies = fetch_top_companies(api_key)

    if companies:
        os.makedirs("data", exist_ok=True)
        filename = "data/codes.csv"

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Symbol",
                # "Company Name",
                # "Market Cap",
                # "Sector",
                # "Industry",
            ])

            for company in companies:
                writer.writerow([
                    company.get("symbol"),
                    # company.get("companyName"),
                    # company.get("marketCap"),
                    # company.get("sector"),
                    # company.get("industry"),
                ])

        print(f"Successfully saved {len(companies)} companies")


if __name__ == "__main__":
    main()