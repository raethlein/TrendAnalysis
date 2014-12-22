from __future__ import division
from collections import Counter, OrderedDict
import logging
import sys
import datetime
import pymongo

log_format = '[%(asctime)-15s] %(levelname)s: %(message)s'
report_interval_minutes = 20
# drop hashtags / mentions occuring lesser / equals than this value over all reports
irrelevant_score = 30

logging.basicConfig(format=log_format, level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger('summary')


def generate_summary(db_reports, summary_db, start, end):
    global report_interval_minutes
    logger.info("Started summary generation for start " + str(start) + " | end: " + str(end))
    # read reports
    reports = db_reports.find({"created_at": {"$gte": start, "$lt": end}})

    # init foo
    cumulatedCounter = Counter()
    hashtag_count = 0
    mention_count = 0
    tweet_count = 0
    deleted_count = 0
    report_count = 0

    stat_obj = {}
    first_stat_count = {}
    previous_stat_count = {}

    # loop over reports for counters and general statistics
    for report in reports:
        report_count += 1
        for stat in report["stats"]:

            cumulatedCounter.update({stat: report["stats"][stat]})
            current_stat_count = report['stats'][stat]

            if stat not in stat_obj:
                stat_obj[stat] = {'differences_abs': {str(report_count): current_stat_count},
                        'differences_relative': {str(report_count): 1},
                        'period_count': {str(report_count): current_stat_count},
                        'total_difference_abs': current_stat_count,
                        'total_difference_relative': 1,
                        'sum': current_stat_count,
                        'max': current_stat_count,
                        'min': current_stat_count,
                        'avg': current_stat_count}
                first_stat_count[stat] = current_stat_count

            else:
                differences_abs = current_stat_count - previous_stat_count[stat]
                stat_obj[stat]['differences_abs'][str(report_count)] = differences_abs

                differences_rel = 1
                if previous_stat_count[stat] != 0:
                    differences_rel = current_stat_count / previous_stat_count[stat]
                stat_obj[stat]['differences_relative'][str(report_count)] = differences_rel

                total_difference_abs = current_stat_count - first_stat_count[stat]
                stat_obj[stat]['total_difference_abs'] = total_difference_abs

                total_difference_rel = 1
                if first_stat_count[stat] != 0:
                    total_difference_rel = current_stat_count / first_stat_count[stat]
                stat_obj[stat]['total_difference_relative'] = total_difference_rel

                stat_obj[stat]['period_count'][str(report_count)] = current_stat_count

                stat_sum = stat_obj[stat]['sum'] + current_stat_count
                stat_obj[stat]['sum'] = stat_sum
                stat_obj[stat]['avg'] = stat_sum / report_count

            if current_stat_count > stat_obj[stat]['max']:
                stat_obj[stat]['max'] = current_stat_count
            if current_stat_count < stat_obj[stat]['min'] or stat_obj[stat]['min'] == 0:
                stat_obj[stat]['min'] = current_stat_count

            previous_stat_count[stat] = report["stats"][stat]

        hashtag_count = hashtag_count + report["hashtags_counter"]
        mention_count = mention_count + report["mentions_counter"]
        tweet_count = tweet_count + report["period_tweet_counter"]

    delete_stat = []
    for stat in stat_obj:
        if stat_obj[stat]['sum'] <= irrelevant_score:
            delete_stat.append(stat)

    for stat in delete_stat:
        del stat_obj[stat]
        deleted_count += 1

    if report_count == 0:
        return

    # calculate general statistics
    average_hashtags_minute = hashtag_count / (report_count * report_interval_minutes)
    average_hashtags_tweet = hashtag_count / tweet_count
    average_mentions_minute = mention_count / (report_count * report_interval_minutes)
    average_mentions_tweet = mention_count / tweet_count
    average_tweet_minute = tweet_count / (report_count * report_interval_minutes)

    # save general statistics into summary
    summary = {}
    summary["created_at"] = datetime.datetime.utcnow()
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
    summary["deleted_count"] = deleted_count

    # sort the stats for hashtags / mentions by sum
    summary.setdefault("stats", {})
    summary["stats"] = OrderedDict(sorted(stat_obj.iteritems(), key=lambda x: x[1]["sum"], reverse=True))
    # summary["_stats"] = None
    summary["deleted_stats"] = deleted_count
    try:
        summary_db.insert(summary)
    except pymongo.errors.DocumentTooLarge:
        logger.info("DocumentTooLarge exception: start " + str(start) + " | end: " + str(end))
        return

    logger.info("Finished summary generation for " + str(tweet_count) + " tweets")
