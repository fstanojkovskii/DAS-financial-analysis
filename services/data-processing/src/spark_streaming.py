from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Valjda nema da ni treba ovoj posle, deka run on pychamr sg

spark = SparkSession.builder \
    .appName("PyCharmSparkStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("date", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", IntegerType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-data") \
    .option("startingOffsets", "earliest") \
    .load()

processed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price_change", col("close") - col("open"))


def write_to_db(batch_df, batch_id):
    (batch_df.write
     .format("org.apache.spark.sql.cassandra")
     .mode("append")
     .options(table="stock_prices", keyspace="financial_data")
     .save())


query = processed_df.writeStream \
    .foreachBatch(write_to_db) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()
