from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

batch_size = 1000
# Load the log file and send each line to the Kafka topic
with open('/Users/aditshrimal/Desktop/MSDS/summer23/assignments/log-analytics/dataset/40MBFile.log', 'r') as f:
    count = 0
    for line in f:
        producer.send('log_topic', line.encode('utf-8'))
        count += 1
        if count % batch_size == 0:
            producer.flush()  # Ensure all messages have been sent
            time.sleep(10)  # delay for 10 seconds

producer.close()
