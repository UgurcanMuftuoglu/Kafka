# Important libraries imported !
from kafka import KafkaProducer
from faker import Faker
import json
from random import randint
import time
faker=Faker('en_US')

bootstrap_servers =['localhost:9092']
topic_name='first_topic'

#Creating Kafka-Producer
producer=KafkaProducer(bootstrap_servers=bootstrap_servers
                      ,value_serializer=lambda v: json.dumps(v).encode('utf-8')
                      )


#Produce data for Producer
for a in range(0, 30):
    #dictToProduce = {}
    #dictToProduce['cf1:age'] = str(randint(0,100))
    Produce=[]
    Produce=str(randint(0,10))
    ack=producer.send(topic_name, key = bytes(a), value = Produce)
    time.sleep(2)


#Printing which partition and topic is using..
metadata = ack.get()
print(metadata.topic)
print(metadata.partition)
