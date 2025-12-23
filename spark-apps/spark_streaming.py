from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    to_timestamp,
    current_timestamp,
    coalesce,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
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
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------
# 🧱 Schema
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
# 📥 Kafka source
# ---------------------------------------
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# ---------------------------------------
# 🧾 JSON parsing
# ---------------------------------------
df_json = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ---------------------------------------
# ⏱️ FIXED timestamp parsing (milliseconds supported)
# ---------------------------------------
df_typed = df_json.withColumn(
    "event_time",
    coalesce(
        to_timestamp(col("timestamp_ist"), "yyyy-MM-dd HH:mm:ss.SSS"),
        current_timestamp(),
    ),
)

# ---------------------------------------
# 🧮 Windowed aggregation
# ---------------------------------------
agg_df = (
    df_typed.withWatermark("event_time", "2 minutes")
    .groupBy(window(col("event_time"), "2 minutes"), col("mac_id"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
    )
)


# ---------------------------------------
# 🗄️ Write to PostgreSQL
# ---------------------------------------
def write_to_postgres(batch_df, batch_id):
    count = batch_df.count()
    print(f"Batch {batch_id} → rows = {count}")

    if count == 0:
        return

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
# ▶️ Start stream
# ---------------------------------------
query = (
    agg_df.writeStream.outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/spark-checkpoints/sensor_avg")
    .start()
)

query.awaitTermination()
