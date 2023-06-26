# Log Analytics Project

This project involves analyzing server logs using big data tools like Apache Kafka, Apache Spark, and Hadoop HDFS. We stream logs from Kafka, process them with Spark Streaming, and store the results in HDFS in the Parquet format for efficient querying and analysis.

## Project Structure

- `dataset/`: This directory contains the log files which will be processed.
- `section-a/`: This directory contains a Jupyter Notebook for exploratory data analysis (EDA) on the log files.
- `section-b/`: This directory is structured as follows:
  - `kafka/`: Contains the Kafka producer script that reads log entries from the files in the `dataset/` directory and sends them to a Kafka topic.
  - `spark-streaming/`: Contains the Spark Streaming script that consumes logs from Kafka, processes them, and writes the results to HDFS.
- `test_parquet.py`: This is a pytest script for validating the parquet files stored in HDFS.

## Setting Up

### Prerequisites

- Apache Kafka
- Apache Spark
- Hadoop
- Python 3.x
- Jupyter Notebook (for EDA)

### Setup Steps

1. Start the Hadoop HDFS services:
    ```
    start-dfs.sh
    ```

2. Start the ZooKeeper service using Homebrew:
    ```
    brew services start zookeeper
    ```

3. Start the Kafka server using Homebrew:
    ```
    brew services start kafka
    ```

4. Run the Kafka log producer (ensure you're in the `section-b/kafka` directory):
    ```
    python publisher.py
    ```

5. Submit the Spark Streaming job (ensure you're in the `section-b/spark-streaming` directory):
    ```
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 consumer.py
    ```

## Running Tests

This project uses Pytest for unit testing. To run the tests, navigate to the project root directory and run:

```
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

---

Stop the Kafka and Zookeeper services using Homebrew when you're done, with the commands:

```
brew services stop kafka
brew services stop zookeeper
```

This ensures the services won't consume resources when they aren't being used.