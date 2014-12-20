#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
import logging
import sys
import datetime
from email.utils import parsedate


#twitter access configuration
consumer_key = "cyQcQzONYkfRg7OPv08mD9G4q"
consumer_secret = "nBHFj5OIMzYteMeyXXhvZXQeIOMFi8TKRY0sBTvqPseowSasAr"
access_token = "1064292907-NG7YvAfYi8vgc2R9LNPyPpTukiNCqI3WbY9A9EE"
access_token_secret = "Hvg9OPVomgmKwG3BUP7K8rlFAEhhQMfn0e80XpYgnffTJ"
dburi = "mongodb://localhost:27017/twitter_sampling"
follow = None
bounding_box_usa = "-124.47,24.0,-66.56,49.3843"
bounding_box_australia = "112.92112,-54.640301,159.2787,-9.22882"
bounding_box_europe = "-31.266001,27.636311,39.869301,81.008797"
locations = bounding_box_usa + "," + bounding_box_australia + "," + bounding_box_europe
language = "en"
firehose = False
track = None
tweets_collection = "tweet_collection"
tweetcounter = 0


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

    class TapStreamer(TwythonStreamer):
        def on_success(self, data):
            if 'text' in data:
                data['created_at'] = parse_datetime(data['created_at'])
                try:
                    data['user']['created_at'] = parse_datetime(data['user']['created_at'])
                except:
                    pass
                try:
                    tweets.insert(data)
                    global tweetcounter
                    tweetcounter = tweetcounter + 1
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
                stream.statuses.filter(locations=locations, language=language)
            else:
                stream.statuses.sample()

        except Exception, e:
            print e.__doc__
            print e.message
        pass

if __name__ == "__main__":
    main(sys.argv)
