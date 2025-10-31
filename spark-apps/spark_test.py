from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("KafkaConnectivityTest")
    .master("spark://spark:7077")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.1,"
        "org.postgresql:postgresql:42.7.3",
    )
    .getOrCreate()
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

df.printSchema()

query = (
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream.format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start()
)

query.awaitTermination()
