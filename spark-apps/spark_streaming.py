from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

# ---------------------------------------
# ⚙️ Spark Configuration
# ---------------------------------------
spark = (
    SparkSession.builder.appName("IoTStreamProcessor")
    .master("spark://spark:7077")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.postgresql:postgresql:42.7.3",
    )
    .config("spark.sql.shuffle.partitions", "10")
    .config("spark.streaming.backpressure.enabled", "true")
    .config("spark.streaming.kafka.maxRatePerPartition", "1000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------
# 🧱 Define Schema (matches your producer)
# ---------------------------------------
schema = StructType(
    [
        StructField("mac_id", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("timestamp_ist", StringType()),
    ]
)

# ---------------------------------------
# 📥 Read from Kafka
# ---------------------------------------
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON from Kafka 'value' field
df_json = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert timestamp string to proper TimestampType
df_typed = df_json.withColumn("timestamp", col("timestamp_ist").cast(TimestampType()))

# ---------------------------------------
# 🧮 Aggregate: Average every 2 minutes per MAC
# ---------------------------------------
agg_df = (
    df_typed.withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "2 minutes"), col("mac_id"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
    )
)


# ---------------------------------------
# 🗄️ Write aggregated results to PostgreSQL
# ---------------------------------------
def write_to_postgres(batch_df, batch_id):
    (
        batch_df.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .write.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/sensordb")
        .option("dbtable", "sensor_avg")
        .option("user", "sparkuser")
        .option("password", "sparkpass")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


# ---------------------------------------
# ▶️ Start Streaming Query
# ---------------------------------------
query = (
    agg_df.writeStream.outputMode("update")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/spark-checkpoints/sensor_avg")
    .start()
)

query.awaitTermination()
