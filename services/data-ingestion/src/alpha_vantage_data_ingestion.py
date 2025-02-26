import requests
import csv
import os
import argparse
import time


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

        # Check if we got valid data
        if "Time Series (Daily)" not in data:
            print(f"Error in response: {data.get('Note', 'Unknown error')}")
            return None

        return data["Time Series (Daily)"]  # Return only the time series data

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Fetch stock data')
    parser.add_argument('symbol', type=str, help='IBM')
    parser.add_argument('--api-key', type=str, required=True, help='API')
    args = parser.parse_args()

    print(f"Fetching data for {args.symbol}...")
    data = fetch_daily_data(args.symbol, args.api_key)

    if data:
        filename = f"{args.symbol}_daily.csv"
        os.makedirs("data", exist_ok=True)

        with open(f"data/{filename}", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])

            for date, values in sorted(data.items()):
                writer.writerow([
                    date,
                    values["1. open"],
                    values["2. high"],
                    values["3. low"],
                    values["4. close"],
                    values["5. volume"]
                ])

        print(f"Saved {len(data)} records to data/{filename}")


if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"Execution time: {time.time() - start_time:.2f}s")