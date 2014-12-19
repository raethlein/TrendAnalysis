#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
from collections import Counter, OrderedDict
import logging
import sys
import datetime
from email.utils import parsedate
from collections import Counter
import time



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

stats_counter = Counter()
period_tweet_counter = 0
mentions_counter = 0
hashtags_counter = 0
TIME_THRESHOLD_SEC = 20 * 60  # seconds until report is persisted
SMALL_SUMMARY_SEC = 2 * 60 * 60
BIG_SUMMARY_SEC = 4 * 60 * 60
start_time = 0
small_summary_start = 0
big_summary_start = 0
report_collection = "report_collection"
summary_collection = "summary_collection"

################################################################
# HACKY CUZ IMPORT NOT WORKING
###########################################
report_interval_minutes = 20
# drop hashtags / mentions occuring lesser / equals than this value over all reports
irrelevant_score = 16

def generate_statistic(db_reports, report_count, summary_db):
    global report_interval_minutes

    # read reports
    reports = db_reports.find().sort("created_at", -1).limit(report_count)

    # init foo
    cumulatedCounter = Counter()
    average_hashtags_minute = -1
    average_hashtags_tweet = -1
    average_mentions_minute = -1
    average_mentions_tweet = -1
    average_tweet_minute = -1
    hashtag_count = 0
    mention_count = 0
    tweet_count = 0
    deleted_count = 0

    # loop over reports for counters and general statistics
    for report in reports:
        for stat in report["stats"]:
            cumulatedCounter.update({stat: report["stats"][stat]})

        hashtag_count = hashtag_count + report["hashtags_counter"]
        mention_count = mention_count + report["mentions_counter"]
        tweet_count = tweet_count + report["period_tweet_counter"]

    # calculate general statistics
    average_hashtags_minute = hashtag_count / (report_count * report_interval_minutes)
    average_hashtags_tweet = hashtag_count / tweet_count
    average_mentions_minute = mention_count / (report_count * report_interval_minutes)
    average_mentions_tweet = mention_count / tweet_count
    average_tweet_minute = tweet_count / (report_count * report_interval_minutes)

    # save general statistics into summary
    summary = {}
    summary["created_at"] = datetime.datetime.now()
    summary["average_hashtags_minute"] = average_hashtags_minute
    summary["average_hashtags_tweet"] = average_hashtags_tweet
    summary["average_mentions_minute"] = average_mentions_minute
    summary["average_mentions_tweet"] = average_mentions_tweet
    summary["average_tweet_minute"] = average_tweet_minute
    summary["total_tweets"] = tweet_count
    summary["total_hashtags"] = hashtag_count
    summary["total_mentions"] = mention_count
    summary["analyzed_reports"] = report_count
    summary["analyzed_time_frame"] = report_count * report_interval_minutes

    # loop over all hashtags / mentions
    for stat in set(cumulatedCounter):
        # drop hashtag / mention if irrelevant
        if cumulatedCounter[stat] <= irrelevant_score:
            del cumulatedCounter[stat]
            deleted_count = deleted_count + 1
        else:
            #  calculate stats per hashtag / mention

            # init foo
            reports.rewind()
            prev_count = 0
            first_count = -1
            last_count = -1
            min_count = 0
            max_count = 0
            sum_count = 0
            differences_absolute = []
            differences_percentage = []

            # collect stats for hashtag / mention from every report
            for report in reports:
                current_count = report["stats"].get(stat, 0)
                if first_count == -1:
                    first_count = current_count

                # calculate differences (growths)
                difference_abs = current_count - prev_count
                difference_per = 1
                if prev_count != 0:
                    difference_per = current_count / prev_count

                # save differences
                differences_absolute.append(difference_abs)
                differences_percentage.append(difference_per)

                # find min / max
                if min_count > current_count:
                    min_count = current_count
                if max_count < current_count:
                    max_count = current_count

                sum_count = sum_count + current_count
                prev_count = current_count
                last_count = current_count

            total_difference_absolute = last_count - first_count
            total_difference_relative = 1
            if first_count != 0:
                total_difference_relative = last_count / first_count

            summary.setdefault("_stats", {})
            summary["_stats"].setdefault(stat, {})
            summary["_stats"][stat]["differences_absolute"] = differences_absolute
            summary["_stats"][stat]["differences_relative"] = differences_percentage
            summary["_stats"][stat]["total_difference_absolute"] = total_difference_absolute
            summary["_stats"][stat]["total_difference_relative"] = total_difference_relative
            summary["_stats"][stat]["minimum"] = min_count
            summary["_stats"][stat]["maximum"] = max_count
            summary["_stats"][stat]["sum"] = sum_count
            summary["_stats"][stat]["average"] = sum_count / report_count

    summary.setdefault("stats", {})
    summary["stats"] = OrderedDict(sorted(summary["_stats"].iteritems(), key=lambda x: x[1]["sum"]))

    summary["_stats"] = None
    summary["deleted_stats"] = deleted_count
    summary_db.insert(summary)



################################################################
# HACKY CUZ IMPORT NOT WORKING
###########################################








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
    report = {'created_at': datetime.datetime.now(),
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


def generate_summary(reports, report_count, summary):
    generate_statistic(reports, report_count, summary)


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

    summary = db[summary_collection]
    summary.ensure_index("created_at", direction=pymongo.ASCENDING)

    class TapStreamer(TwythonStreamer):
        def on_success(self, data):
            global TIME_THRESHOLD_SEC, SMALL_SUMMARY_SEC, BIG_SUMMARY_SEC, start_time, small_summary_start, big_summary_start, period_tweet_counter

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

                    if (time.time() - small_summary_start) > SMALL_SUMMARY_SEC:
                        generate_summary(reports, 6, summary)
                        small_summary_start = time.time()

                    if (time.time() - big_summary_start) > BIG_SUMMARY_SEC:
                        generate_summary(reports, 12, summary)
                        big_summary_start = time.time()

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

    global start_time, small_summary_start, big_summary_start
    start_time = time.time()
    small_summary_start = time.time()
    big_summary_start = time.time()

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
