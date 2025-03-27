from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("CassandraDirectWrite") \
    .config("spark.pyspark.python", "~/Projects/DAS-financial-analysis/.venv/bin/python") \
        .config("spark.pyspark.driver.python", "~/Projects/DAS-financial-analysis/.venv/bin/python") \
        .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

test_df = spark.createDataFrame([("TEST", "2023-11-20")], ["symbol", "date"])
test_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="stock_prices", keyspace="financial_data") \
    .mode("append") \
    .save()
print("Connection test record written")

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("date", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", IntegerType(), True),
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
    .withColumn("price_change", round(col("close") - col("open"), 2))


def write_to_cassandra(batch_df, batch_id):
    print(f"Writing batch {batch_id} ({batch_df.count()} rows)")
    batch_df.printSchema()
    batch_df.show(5)

    (batch_df.write
     .format("org.apache.spark.sql.cassandra")
     .options(table="stock_prices", keyspace="financial_data")
     .mode("append")
     .save())


query = processed_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append")  \
    .option("checkpointLocation", "/tmp/spark-checkpoints-new") \
    .start()

query.awaitTermination()