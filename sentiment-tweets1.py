# -*- coding: utf-8 -*-
"""
Created on Sun Dec 25 15:30:25 2022

@author: Salma EZZINA and Chaima Elmessai
"""

#First approach:
#to classify messages as positive and negative by using the huggingFace transformers:

from transformers import pipeline
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
nltk.download("punkt")
nltk.download("stopwords")
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

import time
topic_eng = 'en-tweets'
topic_fr = 'fr-tweets'

neg = 'negative-tweets'
pos = 'positive-tweets'
counter_pos=0
counter_neg=0 

sentiment_pipeline = pipeline("sentiment-analysis")


#removing useless information in the tweets that won't serve in sentiment analysis
def preprocessing(text, lang): 
    text = text.lower()
    text = re.sub("https?://[\S]+", "", text) #remove links
    text = re.sub(r"@[\S]+", "", text) #remove tags
    text = re.sub("[^a-zA-Z]", " ", text) #remove numbers 
    text = word_tokenize(text) #tokenize the text
    text = [word for word in text if word not in stopwords.words(lang)] #remove stop words
    text = " ".join(word for word in text)
    return text

consumer = KafkaConsumer( bootstrap_servers="localhost:9092", value_deserializer=lambda x: json.loads(x.decode("utf8")),)
topics_list =[topic_eng, topic_fr]
producer = KafkaProducer(bootstrap_servers="localhost:9092",)
consumer.subscribe(topics= topics_list)
print(producer.bootstrap_connected()) 

for message in consumer:    
    if message.topic == topic_eng:
        lang= 'english' 
    elif message.topic == topic_fr:
        lang= 'frensh' 
    msg= preprocessing(message.value['text'], lang)
    print(msg)
    if sentiment_pipeline(msg)[0]['label']== 'POSITIVE' :
        producer.send(pos, bytes(str(message),encoding='utf8')).get(timeout=100)
        counter_pos = counter_pos+1
        print("we Sent to positive-tweets message number: ",counter_pos)

    if sentiment_pipeline(msg)[0]['label']== 'NEGATIVE' :
        producer.send(neg, bytes(str(message),encoding='utf8')).get(timeout=100)
        counter_neg = counter_neg+1
        print("we Sent to negative-tweets message number: ",counter_neg)
        
    time.sleep(1)    