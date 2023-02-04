The work was done by -Salma EZZINA and Chaima Elmessai-

First thing is to launch the zookeeper and the kafka servers:
i. .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
ii. .\bin\windows\kafka-server-start.bat .\config\server.properties

Second is to create the 5 topics: raw-tweets, en-tweets, fr-tweets, positive-tweets, negative-tweets
.\bin\windows\kafka-topics.bat --create --topic raw-tweets --bootstrap-server localhost:9092

Finally,
The folder contains 6 .py files which are in order:
1- ingest-tweets: to scrap tweets and send them to the topic 
'raw-tweets' via kafka producer
2- filter-tweets: to filter the latter twitter based on the language 
(frensh or english) and send them to the 2 topics en-tweets and fr-tweets
3-sentiment-tweets1: To filter tweets based on words used into positive and negative tweets
the first approach used in this file is an NLP approach using HuggingFace transformers
and some  nlp preprocessing for better results
4- sentiment-tweets2: which classifies tweets based on the key: possibly-sensitive
5- archive-data: To save data from the 5 topics locally to 5 .txt files
6- monitor-kafka: to display some info about the messages from the 5 topics