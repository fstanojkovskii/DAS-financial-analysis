import gevent.monkey

gevent.monkey.patch_all()  # Patch standard library

import pandas as pd
from cassandra.cluster import Cluster
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
import os
import multiprocessing
import sys  # For flushing output
import time  # For testing
from datetime import datetime
import cassandra


def connect_to_cassandra():
    cluster = Cluster(['localhost'], protocol_version=4)  # IMPORTANT
    session = cluster.connect('financial_data')
    return cluster, session


def check_existing_data(session, symbol, date):
    try:
        query = "SELECT COUNT(*) FROM analysis_data WHERE symbol = %s AND date = %s"
        result = session.execute(query, (symbol, date))
        return result.one()[0] > 0
    except Exception as e:
        print(f"Error in check_existing_data: {e}")
        sys.stdout.flush()
        return False


def fetch_indicators(session, symbol, date):
    try:
        query = "SELECT ma_5, ma_10, volatility FROM indicators_data WHERE symbol = %s AND date = %s"
        result = session.execute(query, (symbol, date))
        row = result.one()
        return (row.ma_5, row.ma_10, row.volatility) if row else (None, None, None)
    except Exception as e:
        print(f"Error in fetch_indicators: {e}")
        sys.stdout.flush()
        return (None, None, None)


def fetch_historical_data(session, symbol, date):
    try:
        query = """
            SELECT date, close FROM stock_prices
            WHERE symbol = %s AND date < %s
            ORDER BY date DESC LIMIT 10;
        """
        result = session.execute(query, (symbol, date))
        return result
    except Exception as e:
        print(f"Error in fetch_historical_data: {e}")
        sys.stdout.flush()
        return None


def perform_ml_analysis(data, model):
    try:
        if len(data) < 10:
            return []

        data['day'] = data.index.dayofyear
        X = data[['day', 'ma_5', 'ma_10', 'volatility']].dropna()
        y = data['close'].loc[X.index]

        if X.empty or y.empty:
            return []

        model.fit(X, y)  # Wrap in try-except
        return model.predict(X).tolist()
    except Exception as e:
        print(f"ML Model failed to train/predict: {type(model).__name__} - {e}")
        sys.stdout.flush()
        return []


def process_record(symbol, date_str, session):  # Processes a  record
    try:
        print(f"process_record started for {symbol} on {date_str}")
        sys.stdout.flush()

        print(f"Date is {date_str}")

        print("check_existing_data starting")
        sys.stdout.flush()
        if check_existing_data(session, symbol, date_str):
            print(f"Skipping existing data for {symbol} on {date_str}")
            sys.stdout.flush()
            print("check_existing_data finished")
            sys.stdout.flush()
            return

        print("check_existing_data finished")
        sys.stdout.flush()

        print(f"Processing {symbol} on {date_str}")
        sys.stdout.flush()

        print("fetch_indicators starting")
        sys.stdout.flush()
        ma_5, ma_10, volatility = fetch_indicators(session, symbol, date_str)
        print("fetch_indicators finished")
        sys.stdout.flush()

        if ma_5 is None:
            print(f"Skipping {symbol} on {date_str} due to missing indicators")
            sys.stdout.flush()
            return

        print("fetch_historical_data starting")
        sys.stdout.flush()
        historical_data = fetch_historical_data(session, symbol, date_str)

        if historical_data is None:
            print(f"Skipping {symbol} on {date_str} due to missing historical_data")
            sys.stdout.flush()
            return

        print("fetch_historical_data finished")
        sys.stdout.flush()
        df = pd.DataFrame(list(historical_data), columns=['date', 'close'])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df['ma_5'] = ma_5
        df['ma_10'] = ma_10
        df['volatility'] = volatility

        print(f"Running ML analysis for {symbol} on {date_str}")
        sys.stdout.flush()
        predictions = {}
        try:
            print("ML Starting")
            sys.stdout.flush()
            predictions["linear_regression"] = perform_ml_analysis(df, LinearRegression())
            predictions["random_forest"] = perform_ml_analysis(df, RandomForestRegressor(n_estimators=100))
            predictions["svm"] = perform_ml_analysis(df, SVR())
            predictions["decision_tree"] = perform_ml_analysis(df, DecisionTreeRegressor())
            print("ML finished")
            sys.stdout.flush()
        except Exception as e:
            print(f"Error during ML analysis: {e}")
            sys.stdout.flush()
            return

        try:
            print("Starting to save")
            save_to_cassandra(session, symbol, date_str, predictions["decision_tree"], predictions["linear_regression"],
                              predictions["random_forest"], predictions["svm"])

            print("Completed save")
            sys.stdout.flush()

        except:
            print(f"Encountered error trying to save to cassandra")
            sys.stdout.flush()

        print(f"Data for {symbol} on {date_str} processed.")
        sys.stdout.flush()

        print(f"process_record finished for {symbol} on {date_str}")
        sys.stdout.flush()

    except Exception as e:
        print(f"General processing error: {e}")
        sys.stdout.flush()


def save_to_cassandra(session, symbol, date, dt, lin_reg, rf, svm):  # Remove the original object to save.
    try:
        query = """
            INSERT INTO analysis_data (symbol, date, dt_forecast,linear_regression_predictions, rf_forecast,
                                       svm_forecast)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        session.execute(query, (
            symbol, date, dt, lin_reg, rf, svm
        ))

    except Exception as e:
        print(f"Error saving to Cassandra: {e}")
        sys.stdout.flush()


def main():
    print("Starting main()")
    sys.stdout.flush()
    cluster, session = connect_to_cassandra()
    query = "SELECT symbol, date FROM stock_prices"
    rows = session.execute(query)

    records = [(row.symbol, row.date) for row in rows]  # All text, now.

    print(f"Number of records: {len(records)}")
    sys.stdout.flush()

    for symbol, date in records:  # All serially
        process_record(symbol, date, session)  # Call for all data

    cluster.shutdown()
    print("Exiting main()")
    sys.stdout.flush()


if __name__ == "__main__":
    main()