import pandas as pd
import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR

# Конекција за Kafka
consumer = KafkaConsumer(
    'stock-data-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Конекција за Cassandra нова табела analysis_data (create)
def save_to_cassandra(record):
    cluster = Cluster(['localhost'])
    session = cluster.connect('financial_data')
    query = """
        INSERT INTO analysis_data (symbol, date, linear_regression_predictions, rf_forecast, svm_forecast)
        VALUES (%s, %s, %s, %s, %s)
    """

    linear_predictions = record["linear_regression_predictions"] if record["linear_regression_predictions"] else None
    rf_forecast = record["rf_forecast"] if record["rf_forecast"] else None
    svm_forecast = record["svm_forecast"] if record["svm_forecast"] else None

    session.execute(query, (record["symbol"], record["date"], linear_predictions, rf_forecast, svm_forecast))
    cluster.shutdown()

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


def process_message(msg):
    data = pd.DataFrame([msg], columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)

    linear_predictions = linear_regression_analysis(data)
    rf_forecast = random_forest_analysis(data)
    svm_forecast = svm_analysis(data)

    record = {
        "symbol": msg['symbol'],
        "date": msg['date'],
        "linear_regression_predictions": linear_predictions.tolist() if linear_predictions is not None else [],
        "rf_forecast": rf_forecast.tolist() if rf_forecast is not None else [],
        "svm_forecast": svm_forecast.tolist() if svm_forecast is not None else []
    }


    save_to_cassandra(record)
    print(f"Data for {msg['symbol']} on {msg['date']} saved to Cassandra.")


def main():
    for msg in consumer:
        process_message(msg.value)

if __name__ == "__main__":
    main()
