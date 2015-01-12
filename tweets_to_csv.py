import pymongo
import re
import os
import sys
import io
import datetime
import csv, codecs, cStringIO

file_location = r'E:\Development\Sourcetree-Workspace\TrendAnalysis\twitter_output.csv'
db_name = 'twitter_sampling'
collection_name = 'tweet_collection'
mongodb_connection = 'mongodb://localhost:27017/'


from pymongo import MongoClient
client = MongoClient(mongodb_connection)

db = client[db_name]
collection = db[collection_name]



class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
    	
    	self.writer.writerow([unicode(s).encode("utf-8") for s in row])
    	
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def retrieve_data():
	start = datetime.datetime.strptime("2014-12-27 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")
	end = datetime.datetime.strptime("2015-01-07 23:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")
	return collection.find({ "$and" : [{ "coordinates" :  {'$ne' : None}},{"created_at": {"$gte": start, "$lt": end}},{ "$or" : [{"entities.user_mentions.screen_name" : "AirAsia" },{"entities.hashtags.text" : "airasia" },{"entities.hashtags.text" : "qz8501" },{"entities.hashtags.text" : "prayforairasia" },{"entities.hashtags.text" : "airasia8501" },{"entities.hashtags.text" : "prayforqz8501" },{"entities.hashtags.text" : "mh370" }]} ]})  
	
def preprocessing_tweets(tweet_str):

   	tweet_str = tweet_str.rstrip("\r\n")
	tweet_str = tweet_str.rstrip("\n")
	tweet_str = tweet_str.replace("\r\n", "")
	tweet_str = tweet_str.replace("\r", "")
	tweet_str = tweet_str.replace("\n", "")
	tweet_str = tweet_str.replace(";", ",")
	tweet_str = tweet_str.replace("\N", "")
	 #tweet_str = re.sub('[@#$<>:%&]', '', tweet_str)     #remove certain characters
	
	return tweet_str

def create_output_file(tweets):
    print 'tweets loaded'
    csv_writer = UnicodeWriter(open(file_location, "wb"),delimiter=';', dialect='excel',  quoting=csv.QUOTE_NONNUMERIC)
    csv_writer.writerow(['mongo_id','tweet_id','tweet_text','user','longitude','latitude','country_code','city_code','creation_date','in_reply_to','lang','is_retweet','retweet_count','favorite_count'])
    tweet_counter = 0 
    for entity in tweets :
        tweet_counter = tweet_counter + 1
        row = []
        row.append(str(entity['_id']))
        row.append(str(entity['id']))
        row.append(unicode(preprocessing_tweets(entity['text'])))
        row.append(str(entity['user']['screen_name']))
        if (entity['coordinates'] is not None):
            row.append(str(entity['coordinates']['coordinates'][0]))
            row.append(str(entity['coordinates']['coordinates'][1]))
        else:
            row.append("")
            row.append("")
        if (entity['place'] is not None):
            row.append(unicode(entity['place']['country_code']))
            row.append(unicode(entity['place']['full_name']))
        else:
            row.append("")
            row.append("")
        row.append(str(entity['created_at']))
        #row.append(unicode(entity['source']))
        row.append(str(entity['in_reply_to_status_id']))
        row.append(str(entity['lang']))
        row.append(False)
        row.append(str(entity['retweet_count']))
        row.append(str(entity['favorite_count']))

        csv_writer.writerow(row)
        
    print 'completed with '+str(tweet_counter)+" tweets"
if __name__ == '__main__':
	create_output_file(retrieve_data())

 
