#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import datetime
from email.utils import parsedate
from collections import Counter
import time


#twitter access configuration
consumer_key = "T8kK5ev2SJfPYrhZYLDC5shx9"
consumer_secret = "uQvD8ROBwbvr6AlIw29kwvyY7lZnpzIwyqnyzQ9YW7F8DXmrjp"
access_token = "2149523042-C0l0ekF7l7VfTg1RxNLMIZ0acn2hvpRxOSbbDiK"
access_token_secret = "LePbLMYh5lUSPNNJVBFJUaZiisAXCXYF8jZ4kSkuii7CL"
dburi = "mongodb://localhost:27017/twitter_sampling"
follow = None
locations = "-124.47,24.0,-66.56,49.3843"
language = "en"
firehose = False
track = None
tweets_collection = "tweet_collection"
tweetcounter = 0

stats_counter = Counter()
period_tweet_counter = 0
mentions_counter = 0
hashtags_counter = 0
TIME_THRESHOLD_SEC = 0.3 * 60  # seconds until report is persisted
start_time = 0
report_collection = "report_collection"


def calc_stats():
    global stats_counter, hashtags_counter, mentions_counter
    for v in stats_counter:
        if v.startswith("#"):
            hashtags_counter = hashtags_counter + 1
        elif v.startswith("@"):
            mentions_counter = mentions_counter + 1


def save_reports(reports):
    global stats_counter, period_tweet_counter, hashtags_counter, mentions_counter
    calc_stats()
    report = {'created_at': time.time(),
              'stats_counter': sum(stats_counter.values()),
              'period_tweet_counter': period_tweet_counter,
              'hashtags_counter': hashtags_counter,
              'mentions_counter': mentions_counter,
              'stats': dict(stats_counter)}
    reports.insert(report)
    stats_counter.clear()
    period_tweet_counter = 0
    hashtags_counter = 0
    mentions_counter = 0


def collect_stats(tweet):
    for hashtag in tweet['entities']['hashtags']:
        stats_counter.update({"#" + hashtag['text']: 1})
    for mention in tweet['entities']['user_mentions']:
        stats_counter.update({"@" + mention['screen_name']: 1})


def main(argv):
    FORMAT = '[%(asctime)-15s] %(levelname)s: %(message)s'
    import pymongo

    def parse_datetime(string):
        return datetime.datetime(*(parsedate(string)[:6]))

    from twython import TwythonStreamer

    loglevel = logging.DEBUG

    logging.basicConfig(format=FORMAT, level=loglevel, stream=sys.stdout)
    logger = logging.getLogger('twitter')
    logger.info("Starting to collect tweets")

    if consumer_key is None or consumer_secret is None or access_token is None or access_token_secret is None:
        logger.fatal("Consumer key, consumer secret, access token and access token secret are all required when using the streaming API.")
        sys.exit(1)

    try:
        client = pymongo.MongoClient(dburi)
    except:
        logger.fatal("Couldn't connect to MongoDB. Please check your --db argument settings.")
        sys.exit(1)

    parsed_dburi = pymongo.uri_parser.parse_uri(dburi)
    db = client[parsed_dburi['database']]

    tweets = db[tweets_collection]

    tweets.ensure_index("id", direction=pymongo.DESCENDING, unique=True)
    tweets.ensure_index([("coordinates.coordinates", pymongo.GEO2D), ])
    tweets.ensure_index("created_at", direction=pymongo.ASCENDING)
    tweets.ensure_index("entities.hashtags", direction=pymongo.ASCENDING)
    tweets.ensure_index("entities.user_mentions.screen_name", direction=pymongo.ASCENDING)

    reports = db[report_collection]
    reports.ensure_index("created_at", direction=pymongo.ASCENDING)

    class TapStreamer(TwythonStreamer):
        def on_success(self, data):
            global TIME_THRESHOLD_SEC, start_time, period_tweet_counter

            if 'text' in data:
                data['created_at'] = parse_datetime(data['created_at'])
                try:
                    data['user']['created_at'] = parse_datetime(data['user']['created_at'])
                except:
                    pass
                try:
                    collect_stats(data)
                    if (time.time() - start_time) > TIME_THRESHOLD_SEC:
                        save_reports(reports)
                        start_time = time.time()

                    tweets.insert(data)
                    global tweetcounter
                    tweetcounter = tweetcounter + 1
                    period_tweet_counter = period_tweet_counter + 1
                    if (tweetcounter % 2000 is 0):
                        logger.debug(str(tweetcounter) + " tweets collected")
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    logger.error("Couldn't save a tweet: " + str(exc_obj))

            if 'limit' in data:
                logger.warn("The filtered stream has matched more Tweets than its current rate limit allows it to be delivered.")

        def on_error(self, status_code, data):
            logger.error("Received error code " + str(status_code) + ".")


    stream = TapStreamer(consumer_key, consumer_secret, access_token, access_token_secret)
    logger.info("Collecting tweets from the streaming API...")

    while True:
        try:
            if locations or language:
                global start_time
                start_time = time.time()
                stream.statuses.filter(locations=locations, language=language)
            else:
                stream.statuses.sample()

        except Exception, e:
            print e.__doc__
            print e.message
        pass

if __name__ == "__main__":
    main(sys.argv)
