import argparse
from kafka import KafkaProducer

def send_messages(file_path, batch_size, topic_name):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Load the log file and send each line to the Kafka topic
    with open(file_path, 'r') as f:
        count = 0
        for line in f:
            producer.send(topic_name, line.encode('utf-8'))
            count += 1
            if count % batch_size == 0:
                producer.flush()  # Ensure all messages have been sent
                # time.sleep(10)  # delay for 10 seconds

    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', default='/Users/aditshrimal/Desktop/MSDS/summer23/assignments/log-analytics/dataset/2GBFIle.log')
    parser.add_argument('--batch_size', default=1000, type=int)
    parser.add_argument('--topic_name', default='log_topic')

    args = parser.parse_args()

    send_messages(args.file_path, args.batch_size, args.topic_name)