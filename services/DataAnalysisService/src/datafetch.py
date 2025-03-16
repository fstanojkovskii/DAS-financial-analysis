import psycopg2
import pandas as pd
from kafka import KafkaProducer
import json
from concurrent.futures import ThreadPoolExecutor

# Конекција со PostgreSQL
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

# Функција што го враќа најновиот достапен датум
def get_latest_date():
    conn = psycopg2.connect(
        dbname="financial_data",
        user="postgres",
        password="dimo",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM stock_prices")  # Најновиот датум
    latest_date = cursor.fetchone()[0]  # Земање на резултатот
    conn.close()
    return latest_date

# Конекција со Kafka
def send_to_kafka(topic, message):
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda m: json.dumps(m).encode("utf-8")
    )
    producer.send(topic, message)
    producer.flush()
    producer.close()

# Проверка дали податокот веќе постои во PostgreSQL
def data_exists(symbol, date):
    conn = psycopg2.connect(
        dbname="financial_data",
        user="postgres",
        password="dimo",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stock_prices WHERE symbol = %s AND date = %s", (symbol, date))
    exists = cursor.fetchone()[0] > 0
    conn.close()
    return exists

# Функција за испраќање на податоци во Kafka за една линија
def send_data_to_kafka(row):
    if data_exists(row['symbol'], row['date']):
        print(f"⚠️ Skipping existing data for {row['symbol']} on {row['date']}")
        return  # Ако податокот веќе постои, го скипуваме

    kafka_message = {
        "symbol": row['symbol'],
        "date": row['date'].strftime('%Y-%m-%d'),  # Конверзија на датум во стринг
        "open": row['open'],
        "high": row['high'],
        "low": row['low'],
        "close": row['close'],
        "volume": row['volume']
    }
    send_to_kafka('stock-data-topic', kafka_message)
    print(f"✅ Sent data for {row['symbol']} on {row['date']}")

def main():
    # Проверка до кој датум се достапни податоците
    latest_date = get_latest_date()
    print(f"📅 Latest available data: {latest_date}")

    # Извлекуваме податоци од PostgreSQL
    data = get_data_from_postgres()

    # Користење на ThreadPoolExecutor за паралелно испраќање на податоци
    with ThreadPoolExecutor(max_workers=100) as executor:
        # Поделување на задачите помеѓу работните нишки
        executor.map(send_data_to_kafka, [row for _, row in data.iterrows()])

if __name__ == "__main__":
    main()
