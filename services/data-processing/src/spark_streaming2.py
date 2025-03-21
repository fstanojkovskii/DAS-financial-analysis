from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from kafka import KafkaProducer
import json

# Сетирање на Spark сесија
spark = SparkSession.builder \
    .appName("PyCharmSparkStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "192.168.1.3") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Дефинирање на шема за податоците
schema = StructType([
    StructField("symbol", StringType()),
    StructField("date", StringType()),
    StructField("open", DoubleType()),
    StructField("price_change", DoubleType()),  # Додавање на price_change
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", IntegerType())
])

# Читање на податоците од Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Обработка на податоците: пресметка на price_change со заокружување на 2 децимали
processed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price_change", round(col("close") - col("open"), 2))  # 🔹 Заокружување на 2 децимали

# Kafka Producer за испраќање на податоците до третотот сервис
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

def write_to_kafka(batch_df, batch_id):
    # topic 'processed-data'
    for row in batch_df.collect():
        message = {
            'symbol': row['symbol'],
            'date': row['date'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'price_change': row['price_change']
        }
        producer.send('processed-data', message)

query = processed_df.writeStream \
    .foreachBatch(write_to_kafka) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()
