# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 13:01:45 2022

@author: Salma EZZINA et Chaima Elmessai
"""

import tweepy
from kafka import KafkaProducer
from time import sleep
import json 


counter = 1 # to debug the message number
topic_name = 'raw-tweets'
client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAGrckgEAAAAAl%2F3KMfbSk%2FXsmHUkYDP1RpXKrcE%3DvEB7pIvEXiFl2EgwuNvZgGfc48w33mIAMvjshMVgAn2p0JRCQZ')
# searching tweets about covid
query = 'covid -is:retweet'
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda x: json.dumps(x).encode("utf8"),)

print(producer.bootstrap_connected()) 

while True:
    for tweet in tweepy.Paginator(client.search_recent_tweets, query=query,
    tweet_fields=[ 'created_at', 'lang', 'possibly_sensitive'], max_results=30).flatten():
        message = {
                "lang": tweet["lang"],
                "possibly_sensitive": tweet["possibly_sensitive"],
                "text": str(tweet),
                "created_at": tweet["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
            }
        print(tweet, tweet['lang'], tweet['possibly_sensitive'])
        print('\n')
        producer.send(topic_name,message)
        print('Sending message',counter)
        counter+=1
        
    sleep(1)