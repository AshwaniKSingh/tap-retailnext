#!/usr/bin/env python3

"""
singer.io RetailNext tap
"""

import argparse
from datetime import datetime
from datetime import timedelta
import sys
import time
import singer
from singer import utils
import pendulum
import requests

LOGGER = singer.get_logger()
EXECUTION_TIME = str(datetime.now())
HREF_GET = 'https://demo.api.retailnext.net/v1/location'
HREF_POST = 'https://demo.api.retailnext.net/v1/datamine'

PARSER = argparse.ArgumentParser()
PARSER.add_argument('--config', action='store', dest='path',
                    help='Path for configuration file')
PARSER.add_argument('--state', action='store', dest='state',
                    help='Specify data load type(currently supported day and minute.)')

STATE = {}

LOCATION_SCHEMA = {'key_properties': ['id'],
                   'properties': {'address': {'type': ['string', 'null']},
                                  'area': {'type': ['string', 'null']},
                                  'attributes': {'type': ['string', 'null']},
                                  'current_utc_offset': {'type': ['string', 'null']},
                                  'date': {'type': ['string', 'null']},
                                  'id': {'type': 'string'},
                                  'name': {'type': ['string', 'null']},
                                  'parent_id': {'type': ['string', 'null']},
                                  'pos_id': {'type': ['string', 'null']},
                                  'store_id': {'type': ['string', 'null']},
                                  'time_zone': {'type': ['string', 'null']},
                                  'time_zone_abbrev': {'type': ['string', 'null']},
                                  'type': {'type': ['string', 'null']}}, \
                  'stream': 'locations', \
                  'type': 'object'}

METRIC_SCHEMA = {'properties': {'execution_date': {'type': ['string', 'null']},
                                'finish': {'type': 'string'},
                                'from_date': {'type': 'string'},
                                'from_time': {'type': 'string'},
                                'index': {'type': 'integer'},
                                'key': {'type': 'number'},
                                'name': {'type': 'string'},
                                'rid': {'type': 'string'},
                                'start': {'type': 'string'},
                                'till_date': {'type': 'string'},
                                'till_time': {'type': 'string'},
                                'type': {'type': 'string'},
                                'validity': {'type': 'string'},
                                'value': {'type': 'number'}}, \
                  'type': 'object'}

MINUTE_LEVEL_METRICS = {"Traffic/Exposure": ["traffic_in", "traffic_out",
                                             "exposure_rate", "passby_capture_rate"],
                        "Engagement": ["dwell_count", "avg_dwell_duration",
                                       "engagement_rate", "wireless_visit_duration"],
                        "Sales": ["net_sales", "sales_transactions_count",
                                  "return_transactions_count", "total_transactions_count",
                                  "avg_transaction_value", "avg_items_per_transaction",
                                  "sales_transactions_value_per_sq_unit",
                                  "sales_transactions_count_per_sq_unit",
                                  "average_unit_retail", "net_items_count"],
                        "Demographics": ["male_rate", "female_rate", "avg_age",
                                         "male_avg_age", "female_avg_age"],
                        "Staff": ["staff_count", "labor_hours", "shopper_per_labor_hour",
                                  "staff_productivity", "staff_traffic_in", "staff_traffic_out"],
                        "Conversion": ["conversion_rate", "dwell_conversion_rate",
                                       "shopper_yield"],
                        "Guest Wi-Fi": ["new_wifi_users"]}

DAY_LEVEL_METRICS = {"day_metrics":["new_visitors_rate", "repeat_visitors_rate",
                                    "visit_frequency", "total_wifi_users",
                                    "repeat_wifi_users", "wifi_uptake"]}

DEFAULT_FILTER_MIN = {"filter": { \
                           "metrics" : [], \
                           "date_ranges" :[{"last_day" : "2016-04-16", "first_day" : "2016-04-11"}],
                           "group_bys" : [{"unit" : "hours", "group" : "time", "value" : 1}],
                           "time_ranges" : [{"from" : "00:00", "until":"12:00"}],
                           "locations" : []
                                },
                      "increment": 15, "type": "day"}


DEFAULT_FILTER_DAY = {"filter": {"time_ranges": [{"type": "store_hours"}],
                                 "group_bys": [{"value": 1, "unit": "days", "group": "date"}],
                                 "date_ranges": [{"first_day": "2017-12-04",
                                                  "last_day": "2017-12-04"}],
                                 "metrics": [],
                                 "locations": []},
                      "increment": 1, "type": "day"}

ARGUMENTS = PARSER.parse_args()
LOGGER = singer.get_logger()

if ARGUMENTS.path is None:
    LOGGER.error('Specify configuration file folder.')
    sys.exit(1)

PATH = ARGUMENTS.path
STATE_PATH = ARGUMENTS.state


#####################################################################
###  Minutes data load code
#####################################################################

def parser(data):
    location_structure = {u'address':'', \
                         u'area':'', \
                         u'attributes':'', \
                         u'current_utc_offset':'', \
                         u'id':'', \
                         u'name':'', \
                         u'parent_id':'', \
                         u'pos_id':'', \
                         u'store_id':'', \
                         u'time_zone':'', \
                         u'time_zone_abbrev':'', \
                         u'type':'', \
                         u'date': pendulum.parse(EXECUTION_TIME).format(utils.DATETIME_FMT)}
    for i in data.keys():
        if i == u'attributes':
            location_structure[i] = str(data[i])
        elif i == u'address':
            location_structure[i] = data[i]['street_address']
        else:
            location_structure[i] = str(data[i])
    return location_structure

def location_extractor(auth):
    locations = requests.get(HREF_GET,
                             auth=(auth['Access Key'], auth['Secret Key']),
                             headers={'User-Agent': auth['user_agent']})
    if locations.status_code not in  (200, 206):
        LOGGER.error("GET %s %s %s ", locations.status_code, locations.url, locations.content)
        sys.exit(1)
    else:
        for i in locations.json()['locations']:
            yield parser(i)
        while locations.status_code == 206:
            next_page = locations.headers['X-Page-Next']
            locations = requests.get(HREF_GET, \
                                    auth=(auth['Access Key'], auth['Secret Key']), \
                                    headers={'X-Page-Start':next_page, 'X-Page-Length':'100',
                                             'User-Agent': auth['user_agent']})
            if locations.status_code not in (200, 206):
                LOGGER.error("GET %s %s %s ", locations.status_code,\
                                                   locations.url, locations.content)
                sys.exit(1)
            else:
                for i in locations.json()['locations']:
                    yield  parser(i)
#####################################################################
###  Minutes data load code
#####################################################################


def headers_min(filters):
    year, month, day, hour, minute = int(filters['date_ranges'][0]['last_day'].split('-')[0]), \
                                     int(filters['date_ranges'][0]['last_day'].split('-')[1]), \
                                     int(filters['date_ranges'][0]['last_day'].split('-')[2]), \
                                     int(filters['time_ranges'][0]['until'].split(':')[0]), \
                                     int(filters['time_ranges'][0]['until'].split(':')[1])
    # Adding 15 minutes intervals
    date_filter = datetime(year, month, day, hour, minute) + \
                  timedelta(minutes=int(STATE['increment']))
    filters['date_ranges'][0]['first_day'] = str(date_filter.date())
    filters['date_ranges'][0]['last_day'] = str(date_filter.date())
    filters['time_ranges'][0]['from'] = str(hour) + ':' + str(minute)
    filters['time_ranges'][0]['until'] = str(date_filter.hour)+':'+ str(date_filter.minute)

def metrics_extractor_min(pids, auth):
    primary_key = 0
    #metrics_schema = utils.load_json(PATH+'/DataStructure/metric.json')
    filters = STATE['filter']
    #metrics = utils.load_json(PATH+'/filters/metrics.json')
    headers_min(filters=filters)
    singer.write_schema('metrics', METRIC_SCHEMA, ["key"])
    for rid in pids:
        for metric in MINUTE_LEVEL_METRICS:
            filters['locations'] = [rid]
            filters['metrics'] = MINUTE_LEVEL_METRICS[metric]
            header = str(filters).replace('\'', '"')
            page = requests.post(HREF_POST, auth=(auth['Access Key'], auth['Secret Key']) \
                                 , data=header, headers={'User-Agent': auth['user_agent']})
            if page.status_code not in (200, 206):
                LOGGER.error('GET %s %s %s ', page.status_code, page.url, page.content)
                sys.exit(1)
            elif 'metrics' not in page.json():
                LOGGER.error('Metrices is not specified')
            elif 'error'  in page.json() or 'error_type' in page.json():
                LOGGER.error('%s', str(page.json()))
                sys.exit(1)
            else:
                for mts in page.json()['metrics']:
                    try:
                        for data in mts['data']:
                            data['finish'] = data['group']['finish']
                            data['start'] = data['group']['start']
                            data['type'] = data['group']['type']
                            data['name'] = mts['name']
                            data['rid'] = rid
                            data['execution_date'] = EXECUTION_TIME
                            data['from_date'] = filters['date_ranges'][0]['first_day']
                            data['till_date'] = filters['date_ranges'][0]['last_day']
                            data['from_time'] = filters['time_ranges'][0]['from']
                            data['till_time'] = filters['time_ranges'][0]['until']
                            del data['group']
                            primary_key = primary_key + 1
                            data['key'] = primary_key
                            singer.write_record('metrics', data)
                    except KeyError:
                        LOGGER.info('No data for metrics %s', str(MINUTE_LEVEL_METRICS[metric]))
        time.sleep(1)
    singer.write_state(STATE)

def start_load_min():
    auth = utils.load_json(PATH)
    LOGGER.info("Staring schema defination.")
    singer.write_schema('locations', LOCATION_SCHEMA, ["id"])
    location_gen = location_extractor(auth)
    records = []
    for record in location_gen:
        records.append(record)
        singer.write_record('locations', record)
    pid = [i['parent_id'] for i in records]
    ids = [i['id'] for i in records]
    leaf_level_ids = [i for i in ids if i not in pid]
    if leaf_level_ids:
        metrics_extractor_min(leaf_level_ids, auth)


#####################################################################
###  Day data load code
#####################################################################


def headers_day(day_filters):
    date = day_filters['date_ranges'][0]['last_day'].split('-')
    next_date = datetime(int(date[0]), int(date[1]), int(date[2])) +\
                timedelta(days=int(STATE['increment']))
    day_filters['date_ranges'][0]['first_day'] = str(next_date).split(' ')[0]
    day_filters['date_ranges'][0]['last_day'] = str(next_date).split(' ')[0]



def metrics_extractor_day(pids, auth):
    primary_key = 0
    filters = STATE['filter']
    #metrics = utils.load_json(PATH+'/filters/day_metrics.json')
    headers_day(day_filters=filters)
    singer.write_schema('metrics', METRIC_SCHEMA, ["key"])
    for rid in pids:
        for metric in DAY_LEVEL_METRICS:
            filters['locations'] = [rid]
            filters['metrics'] = DAY_LEVEL_METRICS[metric]
            header = str(filters).replace('\'', '"')
            page = requests.post(HREF_POST, auth=(auth['Access Key'], auth['Secret Key']) \
                                 , data=header, headers={'User-Agent': auth['user_agent']})
            if page.status_code not in (200, 206):
                LOGGER.error('GET %s %s %s ', page.status_code, page.url, page.content)
                sys.exit(1)
            elif 'metrics' not in page.json():
                LOGGER.error('Metrices is not specified')
            elif 'error'  in page.json() or 'error_type' in page.json():
                LOGGER.error('%s', str(page.json()))
                sys.exit(1)
            else:
                for mts in page.json()['metrics']:
                    try:
                        for data in mts['data']:
                            data['finish'] = data['group']['finish']
                            data['start'] = data['group']['start']
                            data['type'] = data['group']['type']
                            data['name'] = mts['name']
                            data['rid'] = rid
                            data['execution_date'] = EXECUTION_TIME
                            data['from_date'] = filters['date_ranges'][0]['first_day']
                            data['till_date'] = filters['date_ranges'][0]['last_day']
                            del data['group']
                            primary_key = primary_key + 1
                            data['key'] = primary_key
                            singer.write_record('metrics', data)
                    except KeyError:
                        LOGGER.info('No data for metrics %s', str(DAY_LEVEL_METRICS[metric]))
    singer.write_state(STATE)

def start_load_day():
    auth = utils.load_json(PATH)
    LOGGER.info("Staring schema defination.")
    location_gen = location_extractor(auth)
    records = []
    singer.write_schema('locations', LOCATION_SCHEMA, ["id"])
    for record in location_gen:
        records.append(record)
        singer.write_record('locations', record)
    pid = [i['parent_id'] for i in records]
    ids = [i['id'] for i in records]
    leaf_level_ids = [i for i in ids if i not in pid]
    if leaf_level_ids:
        metrics_extractor_day(leaf_level_ids, auth)

def main():
    global STATE
    if STATE_PATH is not None:
        STATE = utils.load_json(STATE_PATH)
        if STATE['type'] == 'day':
            LOGGER.info('Started data load for daily level metrics')
            start_load_day()
        elif STATE['type'] == 'minute':
            LOGGER.info('Started data load for minutes level metrics')
            start_load_min()
        else:
            LOGGER.error('Load type should me minute or day')

    else:
        LOGGER.info('--state option is not passed running tap with default options')
        STATE = DEFAULT_FILTER_MIN
        start_load_min()
        LOGGER.info('Minute Level info done')
        STATE = DEFAULT_FILTER_DAY
        start_load_day()
        LOGGER.info('Day Level Filter Done')

if __name__ == "__main__":
    main()
