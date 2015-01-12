import pymongo
import re
import os
import sys
import io

file_location = r'E:\Development\Sourcetree-Workspace\TrendAnalysis\twitter_sentiment_output'
db_name = 'twitter_sampling'
collection_name = 'tweet_collection'
mongodb_connection = 'mongodb://localhost:27017/'


from pymongo import MongoClient
client = MongoClient(mongodb_connection)

db = client[db_name]
collection = db[collection_name]


def retrieve_data():
	return collection.find({ "$or" : [{"entities.user_mentions.screen_name" : "AskPlayStation" },{"entities.user_mentions.screen_name" : "KimDotcom" },{"entities.user_mentions.screen_name" : "FUCKCRUCIFIX" },{"entities.user_mentions.screen_name" : "MEGAprivacy" },{"entities.user_mentions.screen_name" : "LizardMafia" },{"entities.user_mentions.screen_name" : "XboxSupport" },{"entities.user_mentions.screen_name" : "PlayStation" },{"entities.user_mentions.screen_name" : "Xbox" },{"entities.hashtags.text" : "psndowntime" },{"entities.hashtags.text" : "playstationsucks" },{"entities.hashtags.text" : "payingfornothing" },{"entities.hashtags.text" : "lizardsquad" },{"entities.hashtags.text" : "psn" },{"entities.hashtags.text" : "playstationnetwork" },{"entities.hashtags.text" : "psndown" },{"entities.hashtags.text" : "finestsquad" },{"entities.hashtags.text" : "lizardpatrol" },{"entities.hashtags.text" : "psnup" },{"entities.hashtags.text" : "xboxlivedown" },{"entities.hashtags.text" : "xboxlive" }]}); 

def preprocessing_tweets(tweet_str):

	tweet_str = re.sub(r'http://[\w.]+/+[\w.]+', "", tweet_str, re.IGNORECASE)    #remove http:// URL shortening links
   	tweet_str = re.sub(r'https://[\w.]+/+[\w.]+',"", tweet_str, re.IGNORECASE)
   	tweet_str = tweet_str.rstrip("\r\n")
	tweet_str = tweet_str.rstrip("\n")
	tweet_str = tweet_str.replace("\r\n", "")
	tweet_str = tweet_str.replace("\r", "")
	tweet_str = tweet_str.replace("\n", "")
	tweet_str = tweet_str.replace("\N", "")
	 #tweet_str = re.sub('[@#$<>:%&]', '', tweet_str)     #remove certain characters
	tweet_str_pp = tweet_str.encode('utf-8').strip()                    
	return tweet_str_pp

def create_output_file(tweets):
	print 'tweets loaded'
	counter = 0
	part = 1
	f = open(file_location+".part"+str(part), 'w')
	f_t = open(file_location+"_ids.part"+str(part), 'w')
	tweet_counter = 0
	for entity in tweets :
		tweet_counter = tweet_counter + 1
		if (counter >= 500000):
			counter = 0
			part = part +1
			f = open(file_location+".part"+str(part), 'w')
			f_t = open(file_location+"_ids.part"+str(part), 'w')

		counter = counter + 1

		tweet_str = preprocessing_tweets(entity['text'])
		f.write(tweet_str+ '\n')
		objectid = str(entity['_id']) 
		f_t.write(objectid + '\n')
    
	print 'completed with '+str(tweet_counter)+" tweets"

if __name__ == '__main__':
	create_output_file(retrieve_data())
	
