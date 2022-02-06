from kafka import KafkaConsumer
from json import loads
from time import sleep

KAFKA_TOPIC = "testTopic"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group1',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    print("Consumer: Received the message {}".format(message))
    sleep(1)