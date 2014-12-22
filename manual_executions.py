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


def test_report_generator():
    global tweet_db, report_db
    start = datetime.datetime.utcnow() - datetime.timedelta(days=3)
    end = datetime.datetime.utcnow()
    report_generator.generate_report(tweet_db, report_db, start, end)


def test_summary_generator():
    global report_db, summary_db
    start = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
    end = datetime.datetime.utcnow()
    summary_generator.generate_summary(report_db, summary_db, start, end)

def test_report_foo_generator():
    global report_foo_db, tweet_db
    start = datetime.datetime.utcnow() - datetime.timedelta(hours=8)
    end = datetime.datetime.utcnow()
    report_generator.generate_report(tweet_db, report_foo_db, start, end)

def test_summary_foo_generator():
    global report_foo_db, summary_foo_db
    start = datetime.datetime.utcnow() - datetime.timedelta(hours=9)
    end = datetime.datetime.utcnow()
    summary_generator.generate_summary(report_foo_db, summary_foo_db, start, end)

# init fooooo!
logging.basicConfig(format=log_format, level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger('manual')

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
tweet_db.ensure_index("entities.hashtags", direction=pymongo.ASCENDING)
tweet_db.ensure_index("entities.user_mentions.screen_name", direction=pymongo.ASCENDING)

report_db = db[report_collection]
report_db.ensure_index("created_at", direction=pymongo.ASCENDING)

report_foo_db = db['report_foo_collection']
report_foo_db.ensure_index("created_at", direction=pymongo.ASCENDING)

summary_db = db[summary_collection]
summary_db.ensure_index("created_at", direction=pymongo.ASCENDING)

summary_foo_db = db['summary_foo_collection']
summary_foo_db.ensure_index("created_at", direction=pymongo.ASCENDING)

# call test methods
#test_report_generator()
#test_summary_generator()
#test_report_foo_generator()
test_summary_foo_generator()