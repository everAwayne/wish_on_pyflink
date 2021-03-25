from kafka import KafkaConsumer
consumer = KafkaConsumer('wish-shop-result-data', bootstrap_servers="10.0.9.5:9092",
                                                 group_id="test_consumer")
for msg in consumer:
    print (msg)
