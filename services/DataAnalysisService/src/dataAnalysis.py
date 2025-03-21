import pandas as pd
from cassandra.cluster import Cluster
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from concurrent.futures import ThreadPoolExecutor

def connect_to_cassandra():
    cluster = Cluster(['localhost'])
    session = cluster.connect('financial_data')
    return cluster, session

def check_existing_data(session, symbol, date):
    query = "SELECT COUNT(*) FROM analysis_data WHERE symbol = %s AND date = %s"
    result = session.execute(query, (symbol, date))
    return result.one()[0] > 0

def fetch_indicators(session, symbol, date):
    query = "SELECT ma_5, ma_10, volatility FROM indicators_data WHERE symbol = %s AND date = %s"
    result = session.execute(query, (symbol, date))
    row = result.one()
    return (row.ma_5, row.ma_10, row.volatility) if row else (None, None, None)

def fetch_historical_data(session, symbol, date):
    query = """
        SELECT date, close FROM stock_prices 
        WHERE symbol = %s AND date < %s 
        ORDER BY date DESC LIMIT 10;
    """
    return session.execute(query, (symbol, date))

def perform_ml_analysis(data, model):
    if len(data) < 10:
        return []

    data['day'] = data.index.dayofyear
    X = data[['day', 'ma_5', 'ma_10', 'volatility']].dropna()
    y = data['close'].loc[X.index]

    if X.empty or y.empty:
        return []

    model.fit(X, y)
    return model.predict(X).tolist()

def process_record(session, record):
    if check_existing_data(session, record.symbol, record.date):
        print(f"Skipping existing data for {record.symbol} on {record.date}")
        return

    print(f"Processing {record.symbol} on {record.date}")
    ma_5, ma_10, volatility = fetch_indicators(session, record.symbol, record.date)

    if ma_5 is None:
        print(f"Skipping {record.symbol} on {record.date} due to missing indicators")
        return

    historical_data = fetch_historical_data(session, record.symbol, record.date)
    df = pd.DataFrame(historical_data, columns=['date', 'close'])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df['ma_5'] = ma_5
    df['ma_10'] = ma_10
    df['volatility'] = volatility

    print(f"Running ML analysis for {record.symbol} on {record.date}")
    predictions = {
        "linear_regression": perform_ml_analysis(df, LinearRegression()),
        "random_forest": perform_ml_analysis(df, RandomForestRegressor(n_estimators=100)),
        "svm": perform_ml_analysis(df, SVR(kernel='rbf')),
        "decision_tree": perform_ml_analysis(df, DecisionTreeRegressor())
    }

    save_to_cassandra(session, record, predictions, ma_5, ma_10, volatility)
    print(f"Data for {record.symbol} on {record.date} processed and saved.")

def save_to_cassandra(session, record, predictions, ma_5, ma_10, volatility):
    query = """
        INSERT INTO analysis_data (symbol, date, linear_regression_predictions, rf_forecast, 
                                   svm_forecast, dt_forecast)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        session.execute(query, (
            record.symbol, record.date, predictions["linear_regression"],
            predictions["random_forest"], predictions["svm"],
            predictions["decision_tree"]
        ))
        print(f"Saved to Cassandra: {record.symbol} - {record.date}")
    except Exception as e:
        print(f"Error saving {record.symbol} - {record.date}: {e}")

def main():
    cluster, session = connect_to_cassandra()
    query = "SELECT symbol, date FROM stock_prices"
    rows = session.execute(query)

    with ThreadPoolExecutor(max_workers=45) as executor:
        for row in rows:
            executor.submit(process_record, session, row)

    cluster.shutdown()

if __name__ == "__main__":
    main()
