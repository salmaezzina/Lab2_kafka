# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 13:04:08 2022

@author: Salma EZZINA et Chaima Elmessai
"""
from kafka import KafkaConsumer


topic_name = 'raw-tweets'
topic_eng = 'en-tweets'
topic_fr = 'fr-tweets'
neg = 'negative-tweets'
pos = 'positive-tweets'

topics_list = [topic_name, topic_eng, topic_fr, neg, pos ]
consumer = KafkaConsumer(bootstrap_servers="localhost:9092", value_deserializer=lambda x: x.decode("utf8"),)


consumer.subscribe(topics=topics_list)
print("consumer subscribed to the 5 topics")

for topic in topics_list:
    path = "{}.txt".format(topic) 
    with open(path, "w") as f:
        pass


Max_records = 99999  # Max records to be saved
counter = 0

for message in consumer:
    topic= message.topic
    path = "{}.txt".format(topic)  
    with open(path, "a") as f:
       f.write(message.value)
       f.write("\n")
       print("message wrote in file")
       
    counter += 1
    if (counter >= Max_records):
        break
         