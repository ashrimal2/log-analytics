from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Load the log file and send each line to the Kafka topic
with open('/Users/aditshrimal/Desktop/MSDS/summer23/assignments/log-analytics/dataset/40MBFile.log', 'r') as f:
    for line in f:
        producer.send('log_topic', line.encode('utf-8'))

producer.close()
