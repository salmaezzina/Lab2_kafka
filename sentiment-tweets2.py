# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 13:03:44 2022

@author: Salma EZZINA et Chaima Elmessai
"""
#The second approach is to use the key: possibly-sensitive
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

import time
topic_eng = 'en-tweets'
topic_fr = 'fr-tweets'

neg = 'negative-tweets'
pos = 'positive-tweets'
counter_pos=1
counter_neg=1

consumer = KafkaConsumer( bootstrap_servers="localhost:9092", value_deserializer=lambda x: json.loads(x.decode("utf8")),)
topics_list =[topic_eng, topic_fr]
producer = KafkaProducer(bootstrap_servers="localhost:9092",)
consumer.subscribe(topics= topics_list)
print(producer.bootstrap_connected()) 

for message in consumer:
    # sensitive ==> positive
    
    if message.value['possibly_sensitive']== True :
        producer.send(pos, bytes(str(message),encoding='utf8')).get(timeout=100)
        counter_pos = counter_pos+1
        print("we Sent to positive-tweets message number ",counter_pos)
        
    if message.value['possibly_sensitive'] == False :
        producer.send(neg, bytes(str(message),encoding='utf8')).get(timeout=100)
        counter_neg = counter_neg+1
        print("we Sent to negative-tweets message number ",counter_neg)
        
    time.sleep(1)    
    