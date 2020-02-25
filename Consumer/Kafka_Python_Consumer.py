#Needed libraries imported .

from kafka import KafkaConsumer
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import json

bootstrap_servers=['localhost:9092']
topic_name='first_topic'

#Creating Kafka Consumer ..
consumer = KafkaConsumer(topic_name
                        ,bootstrap_servers=bootstrap_servers
                        ,auto_offset_reset='latest'
                        #,key_deserializer=lambda item: json.loads(item.decode('utf-8'))
                        ,value_deserializer=lambda item: json.loads(item.decode('utf-8'))
                        )

#Defining a figures shape and location..
fig = plt.figure()
axes1 = fig.add_axes([0.1,0.1,0.8,0.8])
plt.ion()

i=1
data_arr=[]

#Reading every single message from topic 
try:
    for message in consumer:
        data=int(message.value)
        data_arr.append(data)
        #print(data)
        y=np.arange(i)
        axes1.plot(y,data_arr,'red')
        plt.pause(0.1)
        i +=1

except KeyboardInterrupt:
    sys.exit()
