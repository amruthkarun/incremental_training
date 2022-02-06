import random
from time import sleep
from json import dumps
from kafka import KafkaProducer

KAFKA_TOPIC = "testTopic"

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

dataset = list(range(1, 101))
sample_data = random.sample(dataset, 20)

for i in sample_data:
    data = {'data' : i}
    producer.send(KAFKA_TOPIC, value=data)
    print("Producer: Data {} sent successfully!".format(data))
    sleep(1)