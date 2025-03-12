from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster

# Cassandra конекција
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

#зачувува податоци во Cassandra
def save_to_cassandra(record):
    session.execute("""
        INSERT INTO stock_prices (symbol, date, open, high, low, close, volume, price_change)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (record["symbol"], record["date"], record["open"], record["high"],
          record["low"], record["close"], record["volume"], record["price_change"]))

#test
for msg in consumer:
    save_to_cassandra(msg.value)
    print(f"✅ Data saved to Cassandra: {msg.value}")
