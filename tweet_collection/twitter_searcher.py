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

# TWITTER SEARCH API PARAMERTERS
language = "en"
tweets_collection = "hebdo_search"
fetch_count = 100
since_id = -1
query = '#JeSuisCharlie+OR+#CharlieHebdo+OR+#ParisShooting+OR+#JeSuisAhmed+OR+#ChalieHebdo+OR+#IamCharlie+OR+#RespectForMuslims+OR+#stopislam+OR+#ParisMarch+OR+#KillAllMuslims+OR+#JeNeSuisPasCharlie+OR+#JeSuisJuif+OR+#MarcheRepublicaine+OR+@Charlie_Hebdo_+OR+#JeSuisKouachi+OR+#OpCharlieHebdo+OR+#ParisAttacks+OR+#KillAllChristians+OR+#JeNeSuisPasCharlie+OR+#FreedomOfSpeech+OR+#NotInMyName+OR+#religionofpeace+OR+Hebdo'
online_tracks_url = 'http://storage.googleapis.com/trend_analysis/hebdo_track.txt'
validation_word = 'jesuischarlie'
# search_hasthags = ['#jesuischarlie']
fetched_tweets_counter = 0
result_type = "recent"

until_date = "2015-01-09"
end_date = "2015-01-14"
starting_since_id = '553030618670305280'
optional_max_id = '553065176497283071'

# LOGGER
FORMAT = '[%(asctime)-15s] %(levelname)s: %(message)s'
loglevel = logging.INFO
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
    global new_since_id

    day = date_str[-2:]
    new_day = int(day) + 1
    if new_day < 10:
        new_day = "0" + str(new_day)
    new_date = date_str[:-2] + str(new_day)
    return new_date

def loadtracks():
    global query
    response = urllib2.urlopen(online_tracks_url)
    online_tracks = response.read()
    if online_tracks is not None and validation_word in online_tracks:
        return online_tracks
    else:
        return query

flag = True
query = loadtracks()
logger.info("Tracks loaded: "+query)

while flag is True:
    try:
        print language

        if optional_max_id is not None:
            result = twitter.search(q=query, count=fetch_count, lang=language, result_type=result_type,
                                    since_id=starting_since_id, max_id=optional_max_id, until=until_date)
        else:
            result = twitter.search(q=query, count=fetch_count, lang=language, result_type=result_type,
                                    since_id=starting_since_id, until=until_date)

        if 'refresh_url' in result['search_metadata']:
                refresh_url_url_params = result['search_metadata']['refresh_url']
                if refresh_url_url_params is not None and len(refresh_url_url_params) != 0:
                    new_since_id = refresh_url_url_params.split('since_id=')[1].split('&')[0]

        logger.info("FIRST DATE SELECTED: | new: " + until_date +" | next_since_id: " + new_since_id + "<<<")
        store_search_results(result['statuses'])
        flag = False
    except twython.exceptions.TwythonRateLimitError as e:
        logger.warning("Rate limit exceeded. Sleeping 1 minute.")
        time.sleep(60)

counter = 0

old_since_id = starting_since_id
date_changed = False
while True:
    counter += 1
    if counter % 250 == 0:
        query = loadtracks()
        logger.info("Reloaded Tracks: "+query+"requests "+str(counter)+"| until_date " + until_date + " | new since id " + new_since_id + " | old since id " + old_since_id + " |next max id "+next_max_id)
        pass

    if (counter % 100 == 0):
        logger.info(""+str(counter)+" requests: until_date " + until_date + " | new since id " + new_since_id + " | old since id " + old_since_id + " |next max id "+next_max_id)

    if not ('search_metadata' in result):
         logger.info("Search metadata not in result: "+query+"requests "+str(counter)+"| until_date " + until_date + " | new since id " + new_since_id + " | old since id " + old_since_id + " |next max id "+next_max_id)
         break
    elif not ('next_results' in result['search_metadata'] or len(result['statuses']) > 0):
        date_changed = True
        until_date = add_day(until_date)
        logger.info(">>> SWITCHED TO NEW DATE: | new: " + until_date +" | new_since_id: " + new_since_id + "<<<")
        if (until_date == end_date):
            logger.warning("THE END DATE IS REACHED " + until_date + " | new since id " + new_since_id + " | old since id " + old_since_id + " | ")
            break

        old_since_id = new_since_id

    try:
        if 'next_results' in result['search_metadata'] and date_changed==False:
            next_results_url_params = result['search_metadata']['next_results']
            if next_results_url_params is not None and len(next_results_url_params) != 0:
                next_max_id = next_results_url_params.split('max_id=')[1].split('&')[0]

                if int(old_since_id) > int(next_max_id):
                    logger.error("NEW SINCE ID GREATER THAN NEXT_MAX_ID! " + until_date + " | " + new_since_id + " | " + old_since_id + " | " + next_max_id)
                    break

                result = twitter.search(q=query, count=fetch_count, lang=language, result_type=result_type,
                                        since_id=old_since_id, max_id=next_max_id, until=until_date)
        else:
            result = twitter.search(q=query, count=fetch_count, lang=language, result_type=result_type, since_id=new_since_id, until=until_date)
            date_changed = False
            if 'refresh_url' in result['search_metadata']:
                refresh_url_url_params = result['search_metadata']['refresh_url']
                if refresh_url_url_params is not None and len(refresh_url_url_params) != 0:
                    new_since_id = refresh_url_url_params.split('since_id=')[1].split('&')[0]

        store_search_results(result['statuses'])
    except twython.exceptions.TwythonRateLimitError as e:
        logger.error("Rate limit exceeded. Sleeping 1 minute.")
        time.sleep(60)
    except KeyError as ke:
        logger.error("key error: " + str(ke.message) +"| until_date " + until_date + " | new since id " + new_since_id + " | old since id " + old_since_id+" |next max id: "+next_max_id)
    except Exception as e:
        logger.error("Exception occurred: " + str(e.message)+"| until_date " + until_date + " | new since id " + new_since_id + " | old since id " + old_since_id+" |next max id: "+next_max_id)