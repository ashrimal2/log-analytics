from pyspark.sql import SparkSession

def test_files_have_records():
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName('ParquetFilesTest') \
        .getOrCreate()

    # Path to the directory on HDFS
    dir_path = 'hdfs://localhost:9000/user/aditshrimal/msds/summer23/de/assignments/log-analytics/output'

    # Read all Parquet files in the directory into a DataFrame
    df = spark.read.parquet(dir_path)

    # Assert that the number of rows is exactly 299999
    assert df.count() == 299999
