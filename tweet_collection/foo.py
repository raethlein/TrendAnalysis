# from collections import Counter
# import time
# import pymongo
#
# dburi = "mongodb://localhost:27017/twitter_sampling"
# stats_counter = Counter()
# TIME_THRESHOLD_SEC = 0.3 * 60  # seconds until report is persisted
# start_time = 0
# report_collection = "report_collection"
#
#
# def save_reports(reports):
#     global stats_counter
#     report = {'created_at': time.time(),
#               'stats_counter': sum(stats_counter.values()),
#               'stats': dict(stats_counter)}
#     reports.insert(report)
#     stats_counter.clear()
#
#
# def collect_stats(tweet):
#     for hashtag in tweet['entities']['hashtags']:
#         stats_counter.update({"#" + hashtag['text']: 1})
#     for mention in tweet['entities']['user_mentions']:
#         stats_counter.update({"@" + mention['screen_name']: 1})
#
# json = {"entities": {
#         "hashtags": [{"text": "foo"}, {"text": "bar"}, {"text": "foo"}],
#         "user_mentions": [{"screen_name": "alice"}, {"screen_name": "bob"}],
#         }}
# print json["entities"]
# collect_stats(json)
# print stats_counter
#
# try:
#     client = pymongo.MongoClient(dburi)
# except:
#     pass
#
# parsed_dburi = pymongo.uri_parser.parse_uri(dburi)
# db = client[parsed_dburi['database']]
# reports = db[report_collection]
# reports.ensure_index("created_at", direction=pymongo.ASCENDING)
# save_reports(reports)
# # if (time.time() - start_time) > TIME_THRESHOLD_SEC:
# #     print "exceeded threshold"
# #     save_reports(json)
# #     start_time = time.time()

stat_obj = {'differences_abs': [],
            'differences_relative': [],
            'total_difference_abs': 0,
            'total_difference_relative': 0,
            'sum': 0,
            'max': 0,
            'min': 0,
            'avg': 0}

previous_stat_count = {}
first_stat_count = {}
max = 0
min = 0
avg = 0
report_count = 0

for report in reports:
    report_count += 1
    for stat in stats:
        cumulatedCounter.update({stat: report["stats"][stat]})
        current_stat_count = report['stats'][stat]
        if current_stat_count > max:
            max = current_stat_count
        if current_stat_count < min:
            min = current_stat_count

        if stat_obj[stat] == None:
            stat_obj[stat] = {'differences_abs': [current_stat_count],
                'differences_relative': [1],
                'total_difference_abs': current_stat_count,
                'total_difference_relative': 1,
                'sum': current_stat_count,
                'max': current_stat_count,
                'min': current_stat_count,
                'avg': current_stat_count}
            first_stat_count[stat] = current_stat_count

        else:
            differences_abs = current_stat_count - previous_stat_count[stat]
            stat_obj[stat]['differences_abs'].append(differences_abs)

            differences_rel = 1
            if previous_stat_count[stat] != 0:
                differences_rel = current_stat_count / previous_stat_count[stat]
            stat_obj[stat]['differences_relative'].append(differences_rel)

            total_difference_abs = current_stat_count - first_stat_count[stat]
            stat_obj[stat]['differences_abs'] = total_difference_abs

            total_difference_rel = 1
            if first_stat_count[stat] != 0:
                total_differences_rel = current_stat_count / first_stat_count[stat]
            stat_obj[stat]['total_difference_rel'] = total_difference_rel

            stat_sum = stat_obj[stat]['sum'] + current_stat_count
            stat_obj[stat]['sum'] += stat_sum
            stat_obj[stat]['max'] = max
            stat_obj[stat]['min'] = min
            stat_obj[stat]['avg'] = stat_sum / report_count

        previous_stat_count[stat] = report["stats"][stat]