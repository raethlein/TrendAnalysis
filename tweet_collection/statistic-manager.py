import sys
import pymongo
import logging
import time
import datetime

import report_generator
import summary_generator

log_format = '[%(asctime)-15s] %(levelname)s: %(message)s'
dburi = "mongodb://localhost:27017/twitter_sampling"
tweet_collection = "tweet_collection"
report_collection = "report_collection"
summary_collection = "summary_collection"
report_interval_minutes = 20
small_summary_interval_minutes = 2 * 60
big_summary_interval_minutes = 4 * 60


def main(argv):
    global log_format, dburi, tweet_collection, report_collection, summary_collection, report_interval_minutes, small_summary_interval_minutes, big_summary_interval_minutes

    logging.basicConfig(format=log_format, level=logging.DEBUG, stream=sys.stdout)
    logger = logging.getLogger('statistics')
    logger.info("Starting statistics manager")

    try:
        client = pymongo.MongoClient(dburi)
    except:
        logger.fatal("Couldn't connect to MongoDB. Please check your --db argument settings.")
        sys.exit(1)

    parsed_dburi = pymongo.uri_parser.parse_uri(dburi)
    db = client[parsed_dburi['database']]

    tweet_db = db[tweet_collection]
    tweet_db.ensure_index("id", direction=pymongo.DESCENDING, unique=True)
    tweet_db.ensure_index([("coordinates.coordinates", pymongo.GEO2D), ])
    tweet_db.ensure_index("created_at", direction=pymongo.ASCENDING)
    tweet_db.ensure_index("entities.hashtags.text", direction=pymongo.ASCENDING)
    tweet_db.ensure_index("entities.user_mentions.screen_name", direction=pymongo.ASCENDING)

    report_db = db[report_collection]
    report_db.ensure_index("created_at", direction=pymongo.ASCENDING)

    summary_db = db[summary_collection]
    summary_db.ensure_index("created_at", direction=pymongo.ASCENDING)

    last_time_report = time.time()
    last_time_small_summary = time.time()
    last_time_big_summary = time.time()

    while True:
        time_since_report = time.time() - last_time_report
        time_since_small_summary = time.time() - last_time_small_summary
        time_since_big_summary = time.time() - last_time_big_summary

        if time_since_report >= (report_interval_minutes * 60):
            start = datetime.datetime.utcnow() - datetime.timedelta(minutes=report_interval_minutes)
            end = datetime.datetime.utcnow()

            report_generator.generate_report(tweet_db, report_db, start, end)
            last_time_report = time.time()

        if time_since_small_summary >= (small_summary_interval_minutes * 60):
            start = datetime.datetime.utcnow() - datetime.timedelta(minutes=small_summary_interval_minutes)
            end = datetime.datetime.utcnow()
            summary_generator.generate_summary(report_db, summary_db, start, end)
            last_time_small_summary = time.time()

        if time_since_big_summary >= (big_summary_interval_minutes * 60):
            start = datetime.datetime.utcnow() - datetime.timedelta(minutes=big_summary_interval_minutes)
            end = datetime.datetime.utcnow()
            summary_generator.generate_summary(report_db, summary_db, start, end)
            last_time_big_summary = time.time()

        # sleep one minute
        time.sleep(60)

if __name__ == "__main__":
    main(sys.argv)