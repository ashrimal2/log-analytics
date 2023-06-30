import pyspark.sql.functions as F
from pyspark.sql.functions import col, regexp_extract, to_timestamp, unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read('/Users/aditshrimal/Desktop/MSDS/summer23/assignments/log-analytics/section-b/spark-streaming/config.ini')

kafka_bootstrap_servers = config['DEFAULT']['KafkaBootstrapServers']
topic_name = config['DEFAULT']['TopicName']
hdfs_output_path = config['DEFAULT']['HDFSOutputPath']
hdfs_checkpoint_path = config['DEFAULT']['HDFSCheckpointPath']

# 1. Create a SparkSession
spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()

# 2. Read from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .option("kafka.group.id", "{}_consumer1".format(topic_name)) \
    .option("failOnDataLoss", "false") \
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

# Timestamp conversion
timestamp_format = "dd/MMM/yyyy:HH:mm:ss Z"
df_parsed = df_parsed.withColumn("timestamp", 
                                 to_timestamp(unix_timestamp(df_parsed["timestamp"], timestamp_format).cast("timestamp")))

# Applying watermark
df_parsed = df_parsed.withWatermark("timestamp", "1 hour")

# 4. Creating EDA parquet files for streaming data
# 4.1. Content Size Analysis
content_analysis = df_parsed\
        .describe(["content_size"])\
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("append") \
        .format("parquet") \
        .option("path", hdfs_output_path+"/content_size") \
        .option("checkpointLocation", hdfs_checkpoint_path) \
        .start()

content_analysis.awaitTermination()

# 4.2. HTTP Status Analysis with watermarking
http_analysis = df_parsed \
    .groupBy("status") \
    .count() \
    .writeStream \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .format("parquet") \
    .option("path", hdfs_output_path+"/http_status") \
    .option("checkpointLocation", hdfs_checkpoint_path) \
    .start()

http_analysis.awaitTermination()

# #4.3 Analysing Frequent hosts
# freq_host_analysis = df_parsed\
#         .groupBy("host").count()\
#         .writeStream \
#         .trigger(processingTime='10 seconds') \
#         .outputMode("append") \
#         .format("parquet") \
#         .option("path", hdfs_output_path+"/freq_host_analysis") \
#         .option("checkpointLocation", hdfs_checkpoint_path) \
#         .start()

# # freq_host_analysis.awaitTermination()

# #4.4 Top 20 Frquent Endpoints
# freq_endpoints = df_parsed\
#         .groupBy("endpoint").count()\
#         .writeStream \
#         .trigger(processingTime='10 seconds') \
#         .outputMode("append") \
#         .format("parquet") \
#         .option("path", hdfs_output_path+"/freq_endpoints") \
#         .option("checkpointLocation", hdfs_checkpoint_path) \
#         .start()

# # freq_endpoints.awaitTermination()

# #4.5 Top 10 Error Endpoints
# freq_error_endpoints = df_parsed\
#         .filter(df_parsed["status"] != 200)\
#         .groupBy("endpoint").count()\
#         .writeStream \
#         .trigger(processingTime='10 seconds') \
#         .outputMode("append") \
#         .format("parquet") \
#         .option("path", hdfs_output_path+"/freq_error_endpoints") \
#         .option("checkpointLocation", hdfs_checkpoint_path) \
#         .start()

# # freq_error_endpoints.awaitTermination()

# #4.6 Average Daily requests per host
# host_day_distinct_df = df_parsed.select(df_parsed.host, F.dayofmonth("timestamp").alias("day")).dropDuplicates()
# daily_hosts_df = (
#     host_day_distinct_df.groupBy("day")
#     .count()
#     .select(col("day"), col("count").alias("total_hosts"))
# )
# total_daily_reqests_df = (
#     df_parsed.select(F.dayofmonth("timestamp").alias("day"))
#     .groupBy("day")
#     .count()
#     .select(col("day"), col("count").alias("total_reqs"))
# )
# avg_daily_reqests_per_host = total_daily_reqests_df.join(daily_hosts_df, "day")\
#         .withColumn(
#             "avg_reqs", col("total_reqs") / col("total_hosts")
#         )\
#         .sort("day")\
#         .writeStream \
#         .trigger(processingTime='10 seconds') \
#         .outputMode("append") \
#         .format("parquet") \
#         .option("path", hdfs_output_path+"/avg_daily_req_per_host") \
#         .option("checkpointLocation", hdfs_checkpoint_path) \
#         .start()

# # avg_daily_reqests_per_host.awaitTermination()

# #4.7 404 Errors per day
# errors_per_day = df_parsed.filter(df_parsed["status"] == 404)\
#         .groupBy(F.dayofmonth("timestamp").alias("day"))\
#         .writeStream \
#         .trigger(processingTime='10 seconds') \
#         .outputMode("append") \
#         .format("parquet") \
#         .option("path", hdfs_output_path+"errors_per_day") \
#         .option("checkpointLocation", hdfs_checkpoint_path) \
#         .start()

# # errors_per_day.awaitTermination()

# #4.8 Frequently accessed endpoints per day of the week
# window = Window.partitionBy("day_of_week").orderBy(F.desc("count"))
# freq_endpoint_day_of_week = df_parsed\
#         .withColumn("day_of_week", F.date_format("timestamp", "E"))\
#         .groupBy("day_of_week", "endpoint").count()\
#         .withColumn("rn", F.row_number().over(window)).filter(
#             F.col("rn") == 1
#         )\
#         .select("day_of_week", "endpoint", "count")\
#         .writeStream \
#         .trigger(processingTime='10 seconds') \
#         .outputMode("append") \
#         .format("parquet") \
#         .option("path", hdfs_output_path+"/freq_endpoints_day_of_week") \
#         .option("checkpointLocation", hdfs_checkpoint_path) \
#         .start()

# # freq_endpoint_day_of_week.awaitTermination()

# #4.9 Total number of 404 status code generated each day of the week.
# total_error_status = df_parsed.filter(F.col("status") == 404)\
#         .groupBy("day_of_week").count()\
#         .writeStream \
#         .trigger(processingTime='10 seconds') \
#         .outputMode("append") \
#         .format("parquet") \
#         .option("path", hdfs_output_path+"/total_error_status") \
#         .option("checkpointLocation", hdfs_checkpoint_path) \
#         .start()

# 5. Write the DataFrame as Parquet files to HDFS every 10 seconds
query = df_parsed \
    .writeStream \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .format("parquet") \
    .option("path", hdfs_output_path) \
    .option("checkpointLocation", hdfs_checkpoint_path) \
    .start()

query.awaitTermination()