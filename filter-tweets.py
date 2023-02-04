# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 13:03:19 2022

@author: Salma EZZINA et Chaima Elmessai
"""

from kafka import KafkaConsumer
from kafka import KafkaProducer
import json

topic_name = 'raw-tweets'
topic_eng = 'en-tweets'
topic_fr = 'fr-tweets'
counter = 1
producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=lambda x: json.dumps(x).encode("utf8"),
                         )
print(producer.bootstrap_connected()) 


consumer =  KafkaConsumer(
    topic_name,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf8")),)



for message in consumer:
    print('here')
    print(message)
    if message.value['lang'] == 'en' :
        producer.send(topic_eng, message.value)
        print("we Sent to en-tweets message number ",counter)
      
    elif message.value['lang'] == 'fr' :
       producer.send(topic_fr, message.value)
       print("we Sent to fr-tweets message number ",counter)
    counter+=1
  