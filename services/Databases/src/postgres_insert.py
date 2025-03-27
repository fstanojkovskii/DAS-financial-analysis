import json
import psycopg2
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv

load_dotenv()

pg_conn = psycopg2.connect(
    dbname="financial_data",
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host="localhost",
    port="5432"
)
pg_cursor = pg_conn.cursor()


pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        symbol VARCHAR(10),
        date DATE,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume INT,
        price_change DOUBLE PRECISION,
        PRIMARY KEY (symbol, date)
    )
""")
pg_conn.commit()

# Kafka Consumer за 'processed-data'
consumer = KafkaConsumer(
    "processed-data",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

def record_exists(symbol, date):
    pg_cursor.execute("""
        SELECT 1 FROM stock_prices WHERE symbol = %s AND date = %s
    """, (symbol, date))
    return pg_cursor.fetchone() is not None
# funkcija save to cassandra ss test
def save_to_postgres(record):
    if not record_exists(record["symbol"], record["date"]):
        pg_cursor.execute("""
            INSERT INTO stock_prices (symbol, date, open, high, low, close, volume, price_change)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (record["symbol"], record["date"], record["open"], record["high"],
              record["low"], record["close"], record["volume"], record["price_change"]))
        pg_conn.commit()
        print(f"✅ Data saved to PostgreSQL: {record}")
    else:
        print(f"⚠️ Skipping existing record: {record['symbol']} - {record['date']}")


def main():
    try:
        for msg in consumer:
            save_to_postgres(msg.value)
    except KeyboardInterrupt:
        print("Program interrupted and stopping...")

    print("All messages processed. Exiting.")
    consumer.close()

if __name__ == "__main__":
    main()
