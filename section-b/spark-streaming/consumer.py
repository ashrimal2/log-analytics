import pyspark.sql.functions as F
from pyspark.sql.functions import col, regexp_extract, to_timestamp, unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import configparser

# Constants
WINDOW_SIZE = "3 hours"
OUTPUT_FORMAT = "parquet"
TRIGGER_PROCESSING_TIME = '10 seconds'

config = configparser.ConfigParser()
config.read('/Users/aditshrimal/Desktop/MSDS/summer23/assignments/log-analytics/section-b/spark-streaming/config.ini')

kafka_bootstrap_servers = config['DEFAULT']['KafkaBootstrapServers']
topic_name = config['DEFAULT']['TopicName']
hdfs_output_path = config['DEFAULT']['HDFSOutputPath']
hdfs_checkpoint_path = config['DEFAULT']['HDFSCheckpointPath']

# 1. Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Read from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Parse the log data into separate columns
df_parsed = df.select(
    regexp_extract("value", r"^([^\s]+\s)", 1).alias("host"),
    regexp_extract(
        "value", r"^.*\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})]", 1
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
timestamp_format = "yyyy-MM-dd HH:mm:ss"
df_parsed = df_parsed.withColumn("timestamp",
                                 to_timestamp(unix_timestamp(df_parsed["timestamp"], timestamp_format).cast("timestamp")))

# Add a watermark
df_parsed = df_parsed.withWatermark("timestamp", "1 day")

# Function to write a DataFrame to a stream
def write_to_stream(df, output_path, checkpoint_path):
    df.writeStream \
        .outputMode("append") \
        .format(OUTPUT_FORMAT) \
        .option("path", hdfs_output_path + output_path) \
        .option("checkpointLocation", hdfs_checkpoint_path + checkpoint_path) \
        .trigger(processingTime=TRIGGER_PROCESSING_TIME) \
        .start()

# 4. Creating EDA parquet files for streaming data
# 4.1. Number of requests per host
requests_per_host = df_parsed.groupBy("host", F.window("timestamp", WINDOW_SIZE)).count()
requests_per_host = requests_per_host.select("host", "window.start", "window.end", "count")
write_to_stream(requests_per_host, "/requests", "/requests")

# 4.2. Number of requests per endpoint
requests_per_endpoint = df_parsed.groupBy("endpoint", F.window("timestamp", WINDOW_SIZE)).count()
requests_per_endpoint = requests_per_endpoint.select("endpoint", "window.start", "window.end", "count")
write_to_stream(requests_per_endpoint, "/endpoints", "/endpoints")

# 4.3. Number of requests per status
requests_per_status = df_parsed.groupBy("status", F.window("timestamp", WINDOW_SIZE)).count()
requests_per_status = requests_per_status.select("status", "window.start", "window.end", "count")
write_to_stream(requests_per_status, "/status", "/status")

# 4.4. Number of requests per content size
requests_per_content_size = df_parsed.groupBy("content_size", F.window("timestamp", WINDOW_SIZE)).count()
requests_per_content_size = requests_per_content_size.select("content_size", "window.start", "window.end", "count")
write_to_stream(requests_per_content_size, "/content_size", "/content_size")

# 4.5. Number of requests per protocol
requests_per_protocol = df_parsed.groupBy("protocol", F.window("timestamp", WINDOW_SIZE)).count()
requests_per_protocol = requests_per_protocol.select("protocol", "window.start", "window.end", "count")
write_to_stream(requests_per_protocol, "/protocol", "/protocol")

# 4.6. Number of requests per method
requests_per_method = df_parsed.groupBy("method", F.window("timestamp", WINDOW_SIZE)).count()
requests_per_method = requests_per_method.select("method", "window.start", "window.end", "count")
write_to_stream(requests_per_method, "/method", "/method")

spark.streams.awaitAnyTermination()