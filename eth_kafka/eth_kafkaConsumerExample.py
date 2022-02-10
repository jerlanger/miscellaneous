from kafka import KafkaConsumer

consumer = KafkaConsumer("eth-market", bootstrap_servers="127.0.0.1:9093")
for msg in consumer:
    print(msg)
