import pandas as pd
from cassandra.cluster import Cluster
from concurrent.futures import ThreadPoolExecutor


def connect_to_cassandra():
    cluster = Cluster(['localhost'])
    session = cluster.connect('financial_data')
    return cluster, session


def check_existing_indicators(session, symbol, date):
    query = "SELECT COUNT(*) FROM indicators_data WHERE symbol = %s AND date = %s"
    result = session.execute(query, (symbol, date))
    return result.one()[0] > 0

def fetch_historical_data(session, symbol, date):
    query = """
        SELECT date, close FROM stock_prices 
        WHERE symbol = %s AND date < %s 
        ORDER BY date DESC LIMIT 10;
    """
    rows = session.execute(query, (symbol, date))
    return rows

def calculate_indicators(historical_data):
    df = pd.DataFrame(historical_data, columns=['date', 'close'])
    if df.empty or len(df) < 10:
        return None, None, None

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    ma_5 = df['close'].rolling(window=5).mean().iloc[-1]
    ma_10 = df['close'].rolling(window=10).mean().iloc[-1]
    volatility = df['close'].rolling(window=10).std().iloc[-1]

    return ma_5, ma_10, volatility

def save_indicators_to_cassandra(session, symbol, date, ma_5, ma_10, volatility):
    query = """
        INSERT INTO indicators_data (symbol, date, ma_5, ma_10, volatility)
        VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (symbol, date, ma_5, ma_10, volatility))

def process_record(session, record):
    if check_existing_indicators(session, record.symbol, record.date):
        print(f"Indicators already exist for {record.symbol} on {record.date}, skipping.")
        return

    historical_data = fetch_historical_data(session, record.symbol, record.date)
    ma_5, ma_10, volatility = calculate_indicators(historical_data)

    if ma_5 is not None:
        save_indicators_to_cassandra(session, record.symbol, record.date, ma_5, ma_10, volatility)
        print(f"Saved indicators for {record.symbol} on {record.date}")

def main():
    cluster, session = connect_to_cassandra()
    query = "SELECT symbol, date FROM stock_prices"  # Отстранет DISTINCT
    rows = session.execute(query)

    with ThreadPoolExecutor(max_workers=50) as executor:
        executor.map(lambda row: process_record(session, row), rows)

    cluster.shutdown()

if __name__ == "__main__":
    main()
