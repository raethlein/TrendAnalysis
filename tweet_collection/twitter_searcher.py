from twython import Twython
import twython
import logging
import sys
import pymongo
import datetime
from email.utils import parsedate
import cleaner
import time
import urllib2

consumer_key = "aUn0208rHLXFoN3Xg4hXu9Lgl"
consumer_secret = "5xaFWqaSQtCuPaExsbtFLLpjvo5Sn7xMjWD3U3k2D0LPoAOjX1"
access_token = "1064292907-CuCx1PIdpOSrBsdeiWUqkqmIdtZD97XJ36lCp4l"
access_token_secret = "Fdb2FzdGvaeXhD746BtO9hQSgOkZIJ4Rpl3jNndbRuSiE"

dburi = "mongodb://localhost:27017/twitter_sampling"

language = "en"
tweets_collection = "hebdo_search"
fetch_count = 100
since_id = -1
query = '#jesuischarlie+OR+#charliehebdo+OR+#jenesuispascharlie+OR+#JeSuisAhmed+OR+#NousSommesCharlie+OR+#MarcheRepublicaine+OR+#RespectforMuslims+OR+#FreedomOfSpeech+OR+#iamcharlie+OR+#KillAllMuslims+OR+#IslamNonCoupable+OR+@Charlie_Hebdo'
online_tracks_url = 'http://storage.googleapis.com/trend_analysis/hebdo_track.txt'
validation_word = 'jesuischarlie'
# search_hasthags = ['#jesuischarlie']
fetched_tweets_counter = 0

# LOGGER
FORMAT = '[%(asctime)-15s] %(levelname)s: %(message)s'
loglevel = logging.DEBUG
logging.basicConfig(format=FORMAT, level=loglevel, stream=sys.stdout)
logger = logging.getLogger('twitter')
logger.info("Starting to collect tweets")

try:
    client = pymongo.MongoClient(dburi)
except Exception:
    logger.fatal("Couldn't connect to MongoDB. Please check your --db argument settings.")
    sys.exit(1)

parsed_dburi = pymongo.uri_parser.parse_uri(dburi)
db = client[parsed_dburi['database']]

tweets = db[tweets_collection]
tweets.ensure_index("id", direction=pymongo.DESCENDING, unique=True)
tweets.ensure_index("text_words", direction=pymongo.DESCENDING)
tweets.ensure_index([("coordinates.coordinates", pymongo.GEO2D), ])
tweets.ensure_index("created_at", direction=pymongo.ASCENDING)
tweets.ensure_index("entities.hashtags.text", direction=pymongo.ASCENDING)
tweets.ensure_index("entities.user_mentions.screen_name", direction=pymongo.ASCENDING)

# twitter = Twython(consumer_key, consumer_secret, access_token, access_token_secret)
twitter = Twython(consumer_key, consumer_secret)

if consumer_key is None or consumer_secret is None or access_token is None or access_token_secret is None:
    logger.fatal("Consumer key, consumer secret, access token and access token secret are all required when using the search API.")
    sys.exit(1)


def parse_datetime(string):
    return datetime.datetime(*(parsedate(string)[:6]))


def clean_tweet_data(tweet):
    tweet['created_at'] = parse_datetime(tweet['created_at'])

    for hashtag in tweet['entities']['hashtags']:
        hashtag['text'] = hashtag['text'].lower()

    try:
        tweet['user']['created_at'] = parse_datetime(tweet['user']['created_at'])
    except:
        pass

    cleaned = cleaner.remove_stop_words(tweet['text'])
    words = str.split(cleaned)
    words = [x.lower() for x in words]
    tweet['text_words'] = words

    return tweet


def store_search_results(data):
    global tweets, fetched_tweets_counter
    for tweet in data:
        try:
            clean_tweet_data(tweet)
            tweets.insert(tweet)
            fetched_tweets_counter += 1
            if fetched_tweets_counter % 500 is 0:
                logger.debug(str(fetched_tweets_counter) + " tweets collected")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logger.error("Couldn't save a tweet: " + str(exc_obj))


def add_day(date_str):
    day = date_str[-2:]
    new_day = int(day) + 1
    if new_day < 10:
        new_day = "0" + str(new_day)
    new_date = date_str[:-2] + str(new_day)
    print "old: " + date_str + " | new: " + new_date
    return new_date

def loadtracks():
    global query
    response = urllib2.urlopen(online_tracks_url)
    online_tracks = response.read()
    if online_tracks is not None and validation_word in online_tracks:
        print "FOOOOOFOOOOOOOOOOOOOOFOFOFOFOFOFO >>> msdlekfdpojeofdje FOOO"
        return online_tracks
    else:
        return query

flag = True
until_date = "2015-01-06"
print query
query = loadtracks()
print query
while flag is True:
    try:
        print language
        result = twitter.search(q=query, count=fetch_count, lang=language, since_id='552166160930988032', until=until_date)
        store_search_results(result['statuses'])
        flag = False
    except twython.exceptions.TwythonRateLimitError as e:
        print "Rate limit exceeded. Sleeping 1 minute."
        time.sleep(60)

counter = 0
while True:
    if counter % 250 == 0:
        query = loadtracks()

    if len(result['statuses']) < fetch_count:
        until_date = add_day(until_date)

    try:
        if 'next_results' in result['search_metadata']:
            next_results_url_params = result['search_metadata']['next_results']
            if next_results_url_params is not None and len(next_results_url_params) != 0:
                next_max_id = next_results_url_params.split('max_id=')[1].split('&')[0]
                result = twitter.search(q=query, count=fetch_count, lang=language, max_id=next_max_id, until=until_date)
        else:
            result = twitter.search(q=query, count=fetch_count, lang=language, until=until_date)

        store_search_results(result['statuses'])
    except twython.exceptions.TwythonRateLimitError as e:
        print "Rate limit exceeded. Sleeping 1 minute."
        time.sleep(60)
    except KeyError as ke:
        print "key error: " + str(ke.message) + " | date: " + until_date
        break;