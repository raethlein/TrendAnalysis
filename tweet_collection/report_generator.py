import datetime
from collections import Counter
import logging
import sys

report_interval_minutes = 20
stats_counter = Counter()
mentions_counter = 0
hashtags_counter = 0
tweet_counter = 0

FORMAT = '[%(asctime)-15s] %(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger('report_generator')

def generate_report(tweets_db, reports_db, start, end):
    global report_interval_minutes, mentions_counter, hashtags_counter, tweet_counter

    logging.info("Started generating reports")

    # init foo
    total_difference_minute = (end - start).total_seconds() / 60
    total_reports_to_generate = total_difference_minute / report_interval_minutes

    for i in range(0, int(total_reports_to_generate)):
        interval_start = start + datetime.timedelta(minutes=(i * report_interval_minutes))
        interval_end = interval_start + datetime.timedelta(minutes=report_interval_minutes)

        # read tweets from last minutes from db
        tweets = tweets_db.find({"created_at": {"$gte": interval_start, "$lt": interval_end}}).sort("created_at", -1)

        # fill counters for tweets
        for tweet in tweets:
            tweet_counter = tweet_counter + 1
            collect_stats(tweet)

        if tweet_counter == 0:
            continue

        # save report
        report = {'created_at': interval_start,
                  'stats_counter': sum(stats_counter.values()),
                  'period_tweet_counter': tweet_counter,
                  'hashtags_counter': hashtags_counter,
                  'mentions_counter': mentions_counter,
                  'stats': dict(stats_counter)}
        reports_db.insert(report)

        # reset counters
        stats_counter.clear()
        hashtags_counter = 0
        mentions_counter = 0
        tweet_counter = 0

        logging.info("Generated report")

def collect_stats(tweet):
    global stats_counter, hashtags_counter, mentions_counter
    for hashtag in tweet['entities']['hashtags']:
        stats_counter.update({"#" + hashtag['text'].lower(): 1})
        hashtags_counter = hashtags_counter + 1
    for mention in tweet['entities']['user_mentions']:
        stats_counter.update({"@" + mention['screen_name'].lower(): 1})
        mentions_counter = mentions_counter + 1