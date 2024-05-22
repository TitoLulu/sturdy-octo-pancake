from kafka import KafkaConsumer
bootstrap_servers = ['localhost:9092']
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

print(consumer.topics())