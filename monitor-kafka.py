# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 21:57:02 2022

@author: Salma EZZINA et Chaima Elmessai
"""

import time
from kafka import KafkaConsumer

topic_eng = 'en-tweets'
topic_fr = 'fr-tweets'
topic_name = 'raw-tweets'
neg = 'negative-tweets'
pos = 'positive-tweets'

topics_list = [topic_name, topic_eng, topic_fr, neg, pos ]
consumer = KafkaConsumer(bootstrap_servers="localhost:9092", group_id="Monitor", value_deserializer=lambda x: x.decode("utf8"),)

#print(consumer.topics())
consumer.subscribe(topics=topics_list)
print("the List of subscribed Topics is ",consumer.subscription())


for message in consumer:
    print(" Topic-Name : {}, Partition-id: {}, Offset-id: {}, timestamp: {}".format(
    message.topic, message.partition, message.offset, message.timestamp))
    time.sleep(0.5)