from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL конекција
pg_conn = psycopg2.connect(
    dbname="financial_data",
    user="postgres",
    password="dimo",
    host="localhost",
    port="5432"
)
pg_cursor = pg_conn.cursor()

pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        symbol VARCHAR(10),
        date DATE,
        open_price DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close_price DOUBLE PRECISION,
        volume INT,
        price_change DOUBLE PRECISION,
        PRIMARY KEY (symbol,date)
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

# Зачувување на податоци во PostgreSQL
def save_to_postgres(record):
    pg_cursor.execute("""
        INSERT INTO stock_prices (symbol, date, open, high, low, close, volume, price_change)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (record["symbol"], record["date"], record["open"], record["high"],
          record["low"], record["close"], record["volume"], record["price_change"]))
    pg_conn.commit()

# Test
for msg in consumer:
    save_to_postgres(msg.value)
    print(f"✅ Data saved to PostgreSQL: {msg.value}")
