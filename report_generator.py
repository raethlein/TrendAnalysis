import datetime
from collections import Counter

report_interval_minutes = 20
stats_counter = Counter()
mentions_counter = 0
hashtags_counter = 0
tweet_counter = 0

def generate_report(tweets_db, reports_db):
    global report_interval_minutes, mentions_counter, hashtags_counter, tweet_counter
    # init foo
    start = datetime.datetime.now() - datetime.timedelta(minutes=report_interval_minutes)
    end = datetime.datetime.now()

    # read tweets from last minutes from db
    tweets = tweets_db.find({"created_at": {"$gte": start, "$lt": end}}).sort("created_at", -1)

    # fill counters for tweets
    for tweet in tweets:
        tweet_counter = tweet_counter + 1
        collect_stats(tweet)

    # save report
    report = {'created_at': datetime.datetime.now(),
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

def collect_stats(tweet):
    global stats_counter, hashtags_counter, mentions_counter
    for hashtag in tweet['entities']['hashtags']:
        stats_counter.update({"#" + hashtag['text']: 1})
        hashtags_counter = hashtags_counter + 1
    for mention in tweet['entities']['user_mentions']:
        stats_counter.update({"@" + mention['screen_name']: 1})
        mentions_counter = mentions_counter + 1