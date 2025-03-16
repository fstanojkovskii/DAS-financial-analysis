import pandas as pd
import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from concurrent.futures import ThreadPoolExecutor

# Конекција за Kafka
consumer = KafkaConsumer(
    'stock-data-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Конекција за Cassandra
def save_to_cassandra(record):
    cluster = Cluster(['localhost'])
    session = cluster.connect('financial_data')

    # Проверка дали записот веќе постои
    query_check = """
        SELECT COUNT(*) FROM analysis_data WHERE symbol = %s AND date = %s
    """
    result = session.execute(query_check, (record["symbol"], record["date"]))
    if result.one()[0] > 0:  # Ако COUNT(*) е поголемо од 0, записот веќе постои
        print(f"⚠️ Skipping existing record: {record['symbol']} - {record['date']}")
        cluster.shutdown()
        return

    query = """
        INSERT INTO analysis_data (symbol, date, linear_regression_predictions, rf_forecast, svm_forecast, linear_svr_forecast, dt_forecast)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    linear_predictions = record["linear_regression_predictions"] if record["linear_regression_predictions"] else None
    rf_forecast = record["rf_forecast"] if record["rf_forecast"] else None
    svm_forecast = record["svm_forecast"] if record["svm_forecast"] else None
    linear_svr_forecast = record["linear_svr_forecast"] if record["linear_svr_forecast"] else None
    dt_forecast = record["dt_forecast"] if record["dt_forecast"] else None

    session.execute(query, (record["symbol"], record["date"], linear_predictions, rf_forecast, svm_forecast, linear_svr_forecast, dt_forecast))
    cluster.shutdown()
    print(f"✅ Data for {record['symbol']} on {record['date']} saved to Cassandra.")

# Линеарна регресија
def linear_regression_analysis(data):
    data['day'] = data.index.dayofyear
    X = data[['day']]
    y = data['close']

    model = LinearRegression()
    model.fit(X, y)

    predictions = model.predict(X)
    return predictions

# Random Forest
def random_forest_analysis(data):
    data['day'] = data.index.dayofyear
    X = data[['day']]
    y = data['close']
    model = RandomForestRegressor(n_estimators=100)
    model.fit(X, y)

    forecast = model.predict(X)
    return forecast

# SVM
def svm_analysis(data):
    data['day'] = data.index.dayofyear
    X = data[['day']]
    y = data['close']

    model = SVR(kernel='rbf')
    model.fit(X, y)

    forecast = model.predict(X)
    return forecast

# Linear Support Vector Regression (SVR)
def linear_svr_analysis(data):
    data['day'] = data.index.dayofyear
    X = data[['day']]
    y = data['close']

    model = SVR(kernel='linear')  # Linear SVR
    model.fit(X, y)

    forecast = model.predict(X)
    return forecast

# Decision Tree
def decision_tree_analysis(data):
    data['day'] = data.index.dayofyear
    X = data[['day']]
    y = data['close']

    model = DecisionTreeRegressor()
    model.fit(X, y)

    forecast = model.predict(X)
    return forecast

def process_message(msg):
    data = pd.DataFrame([msg], columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)

    linear_predictions = linear_regression_analysis(data)
    rf_forecast = random_forest_analysis(data)
    svm_forecast = svm_analysis(data)
    linear_svr_forecast = linear_svr_analysis(data)  # Use Linear SVR here
    dt_forecast = decision_tree_analysis(data)

    record = {
        "symbol": msg['symbol'],
        "date": msg['date'],
        "linear_regression_predictions": linear_predictions.tolist() if linear_predictions is not None else [],
        "rf_forecast": rf_forecast.tolist() if rf_forecast is not None else [],
        "svm_forecast": svm_forecast.tolist() if svm_forecast is not None else [],
        "linear_svr_forecast": linear_svr_forecast.tolist() if linear_svr_forecast is not None else [],  # Add Linear SVR
        "dt_forecast": dt_forecast.tolist() if dt_forecast is not None else []
    }

    save_to_cassandra(record)
    print(f"Data for {msg['symbol']} on {msg['date']} processed.")

def main():
    with ThreadPoolExecutor(max_workers=80) as executor:
        # Process messages from Kafka concurrently using thread pool
        for msg in consumer:
            executor.submit(process_message, msg.value)

if __name__ == "__main__":
    main()