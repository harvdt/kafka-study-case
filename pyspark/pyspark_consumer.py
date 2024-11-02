from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit
from pyspark.sql.types import StructType, StringType, FloatType

# Configure Spark session for local execution
spark = SparkSession.builder \
    .appName("SensorDataProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()

# Disable verbose logging
spark.sparkContext.setLogLevel("ERROR")

# Create Kafka stream
sensor_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu") \
    .load()

# Define schema for JSON data
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("suhu", FloatType())

# Convert Kafka value to structured data
sensor_df = sensor_data \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.sensor_id", "data.suhu")

# Filter for temperatures above 80°C and add "°C" suffix
alert_df = sensor_df.filter(sensor_df.suhu > 80)
alert_df = alert_df.withColumn("suhu", concat(col("suhu").cast("string"), lit("°C")))

# Display alerts in console
query = alert_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
