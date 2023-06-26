from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# 1. Create a SparkSession
spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()

# 2. Read from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "log_topic") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# 3. Parse the log data into separate columns
df_parsed = df.select(
    regexp_extract("value", r"^([^\s]+\s)", 1).alias("host"),
    regexp_extract(
        "value", r"^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]", 1
    ).alias("timestamp"),
    regexp_extract("value", r'^.*"\s*(\w+)\s+([^\s]+)\s+([^\s]+)"', 1).alias("method"),
    regexp_extract("value", r'^.*"\s*(\w+)\s+([^\s]+)\s+([^\s]+)"', 2).alias(
        "endpoint"
    ),
    regexp_extract("value", r'^.*"\s*(\w+)\s+([^\s]+)\s+([^\s]+)"', 3).alias(
        "protocol"
    ),
    regexp_extract("value", r'^.*"\s+([^\s]+)', 1).cast("integer").alias("status"),
    regexp_extract("value", r"^.*\s+(\d+)$", 1).cast("integer").alias("content_size"),
)

# 4. Write the DataFrame as Parquet files to HDFS every 10 seconds
query = df_parsed \
    .writeStream \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/aditshrimal/msds/summer23/de/assignments/log-analytics/output") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/aditshrimal/msds/summer23/de/assignments/log-analytics/checkpoint") \
    .start()

query.awaitTermination()