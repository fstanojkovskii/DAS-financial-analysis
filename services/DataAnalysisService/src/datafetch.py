import psycopg2
import pandas as pd
from kafka import KafkaProducer
import json

# Конекција со PostgreSQL Ставено може да се смени
def get_data_from_postgres():
    conn = psycopg2.connect(
        dbname="financial_data",
        user="postgres",
        password="dimo",
        host="localhost",
        port="5432"
    )
    query = "SELECT * FROM stock_prices"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Конекција со Kafka
def send_to_kafka(topic, message):
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda m: json.dumps(m).encode("utf-8")
    )
    producer.send(topic, message)
    producer.flush()
    producer.close()

def main():
    # Извлекуваме податоци од PostgreSQL
    data = get_data_from_postgres()

    # Испраќање на податоци во Kafka за анализа
    for idx, row in data.iterrows():
        kafka_message = {
            "symbol": row['symbol'],
            "date": row['date'].strftime('%Y-%m-%d'),  #date у string
            "open": row['open'],
            "high": row['high'],
            "low": row['low'],
            "close": row['close'],
            "volume": row['volume']
        }
        send_to_kafka('stock-data-topic', kafka_message)
        print(f"Sent data for {row['symbol']} on {row['date']}")

if __name__ == "__main__":
    main()
