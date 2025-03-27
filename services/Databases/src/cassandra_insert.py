from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from concurrent.futures import ThreadPoolExecutor

cluster = Cluster(['localhost'])
session = cluster.connect()
session.set_keyspace('financial_data')

# Kafka Consumer за 'processed-data'
consumer = KafkaConsumer(
    "processed-data",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

def record_exists(symbol, date):
    query = session.execute("""
        SELECT COUNT(*) FROM stock_prices WHERE symbol = %s AND date = %s
    """, (symbol, date))
    return query.one()[0] > 0

# funkcija save to cassandra ss test
def save_to_cassandra(record):
    if not record_exists(record["symbol"], record["date"]):
        session.execute("""
            INSERT INTO stock_prices (symbol, date, open, high, low, close, volume, price_change)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (record["symbol"], record["date"], record["open"], record["high"],
              record["low"], record["close"], record["volume"], record["price_change"]))
        print(f"✅ Data saved: {record}")
    else:
        print(f"⚠️ Skipping existing record: {record['symbol']} - {record['date']}")

def process_message(msg):
    save_to_cassandra(msg.value)

with ThreadPoolExecutor(max_workers=60) as executor:
    for msg in consumer:
        executor.submit(process_message, msg)
