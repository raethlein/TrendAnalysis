import pymongo
import logging
import sys
from collections import Counter


def generate_statistic(db_reports, report_count):
    reports = db_reports.find().sort("created_at", -1).limit(report_count)
    cumulatedCounter = Counter()
    for report in reports:
        for stat in report["stats"]:
            cumulatedCounter.update({stat: report["stats"][stat]})

    summary = {}
    for stat in set(cumulatedCounter):
        if cumulatedCounter[stat] == 1:
            del cumulatedCounter[stat]
        else:
            reports.rewind()
            prev_count = 0
            differences_absolute = []
            differences_percentage = []
            for report in reports:
                current_count = report["stats"].get(stat, 0)
                difference_abs = current_count - prev_count

                difference_per = 1
                if prev_count != 0:
                    difference_per = current_count / prev_count

                differences_absolute.append(difference_abs)
                differences_percentage.append(difference_per)
                prev_count = current_count

            summary.setdefault(stat, {})
            summary[stat]["differencesAbsolute"] = differences_absolute
            summary[stat]["differencesRelative"] = differences_percentage

    print cumulatedCounter
    print summary




# dburi = "mongodb://localhost:27017/twitter_sampling"
# report_collection = "report_collection"
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
# generate_statistic(reports_db, 2)
