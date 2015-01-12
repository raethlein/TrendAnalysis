import pymongo
import re
import os
import sys
import io

import nltk
nltk.download()

from nltk.corpus import stopwords

cachedStopWords = stopwords.words("english")


file_location = r'E:\Development\Sourcetree-Workspace\TrendAnalysis\wordcloud_output.txt'
db_name = 'twitter_sampling'
collection_name = 'tweet_collection'
mongodb_connection = 'mongodb://localhost:27017/'



from pymongo import MongoClient
client = MongoClient(mongodb_connection)

db = client[db_name]
collection = db[collection_name]


def retrieve_data():
	return collection.find({ "$or" : [{"entities.user_mentions.screen_name" : "AirAsia" },{"entities.hashtags.text" : "airasia" },{"entities.hashtags.text" : "qz8501" },{"entities.hashtags.text" : "prayforairasia" },{"entities.hashtags.text" : "airasia8501" },{"entities.hashtags.text" : "prayforqz8501" },{"entities.hashtags.text" : "mh370" }]}); 

def preprocessing_tweets(tweet_str):

	tweet_str = re.sub(r'http://[\w.]+/+[\w.]+', "", tweet_str, re.IGNORECASE)    #remove http:// URL shortening links
   	tweet_str = re.sub(r'https://[\w.]+/+[\w.]+',"", tweet_str, re.IGNORECASE)
   	tweet_str = tweet_str.rstrip("\r\n")
	tweet_str = tweet_str.rstrip("\n")
	tweet_str = tweet_str.replace("\r\n", "")
	tweet_str = tweet_str.replace("\r", "")
	tweet_str = tweet_str.replace("\n", "")
	tweet_str = tweet_str.replace("\N", "")
	tweet_str = re.sub('[^A-Za-z\s]+', ' ', tweet_str)
	 #tweet_str = re.sub('[@#$<>:%&]', '', tweet_str)     #remove certain characters
	tweet_str = ' '.join([word for word in tweet_str.split() if word not in cachedStopWords])
	tweet_str = tweet_str.replace(" amp ", "")
	tweet_str_pp = tweet_str.encode('utf-8').strip()                    
	return tweet_str_pp

def create_output_file(tweets):
	print 'tweets loaded'
	f = open(file_location, 'w')
	tweet_counter = 0 
	for entity in tweets :
		tweet_counter = tweet_counter + 1
		tweet_str = preprocessing_tweets(entity['text'])
		f.write(tweet_str+ ' ')

	print 'completed with '+str(tweet_counter)+" tweets"

if __name__ == '__main__':
	create_output_file(retrieve_data())
	
