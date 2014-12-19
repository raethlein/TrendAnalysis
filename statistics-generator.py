from __future__ import division
from collections import Counter, OrderedDict
from operator import itemgetter, attrgetter

import pymongo
import logging
import sys
import datetime


dburi = "mongodb://localhost:27017/twitter_sampling"
report_collection = "report_collection"
summary_collection = "summary_collection"
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




# loglevel = logging.DEBUG
# FORMAT = '[%(asctime)-15s] %(levelname)s: %(message)s'
#
# logging.basicConfig(format=FORMAT, level=loglevel, stream=sys.stdout)
# logger = logging.getLogger('twitter')
# parsed_dburi = pymongo.uri_parser.parse_uri(dburi)
# try:
#         client = pymongo.MongoClient(dburi)
# except:
#     logger.fatal("Couldn't connect to MongoDB. Please check your --db argument settings.")
#     sys.exit(1)
#
# db = client[parsed_dburi['database']]
# reports_db = db[report_collection]
# reports_db.ensure_index("created_at", direction=pymongo.ASCENDING)
#
# summary_db = db[summary_collection]
# summary_db.ensure_index("created_at", direction=pymongo.ASCENDING)
#
# generate_statistic(reports_db, 10, summary_db)
