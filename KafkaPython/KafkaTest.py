from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='master:9092', auto_offset_reset='latest', consumer_timeout_ms=1000, group_id='idpl-consumer', enable_auto_commit=True)

for message in consumer:
    print ("Vlaue : "+ int(message.value)+"Offset : " + message.offset)

print("Consumer Checked")