#!/usr/bin/env python3

"""
singer.io RetailNext tap
"""

import argparse
from datetime import datetime
from datetime import timedelta
import sys
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
PARSER.add_argument('--loadtype', action='store', dest='operationtype',
                    help='Specify data load type(currently supported day and minute.)')


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

ARGUMENTS = PARSER.parse_args()
LOGGER = singer.get_logger()

if ARGUMENTS.path is None:
    LOGGER.error('Specify configuration file folder.')
    sys.exit(1)

PATH = ARGUMENTS.path
OPERATION_TYPE = ARGUMENTS.operationtype.strip()

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
                             auth=(auth['Access Key'], auth['Secret Key']))
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
                                    headers={'X-Page-Start':next_page, 'X-Page-Length':'100'})
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
    date_filter = datetime(year, month, day, hour, minute) + timedelta(minutes=15)
    filters['date_ranges'][0]['first_day'] = str(date_filter.date())
    filters['date_ranges'][0]['last_day'] = str(date_filter.date())
    filters['time_ranges'][0]['from'] = str(hour) + ':' + str(minute)
    filters['time_ranges'][0]['until'] = str(date_filter.hour)+':'+ str(date_filter.minute)

def metrics_extractor_min(pids, auth):
    primary_key = 0
    #metrics_schema = utils.load_json(PATH+'/DataStructure/metric.json')
    filters = utils.load_json(PATH+'/filters/filters.json')
    metrics = utils.load_json(PATH+'/filters/metrics.json')
    headers_min(filters=filters)
    singer.write_schema('metrics', METRIC_SCHEMA, ["key"])
    for rid in pids:
        for metric in metrics:
            filters['locations'] = [rid]
            filters['metrics'] = metrics[metric]
            header = str(filters).replace('\'', '"')
            page = requests.post(HREF_POST, auth=(auth['Access Key'], auth['Secret Key']) \
                                 , data=header)
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
                        LOGGER.info('No data for metrics %s', str(metrics[metric]))
    filters_file = open(PATH+'/filters/filters.json', 'w')
    filters_file.write(header)
    filters_file.close()

def start_load_min():
    auth = utils.load_json(PATH+'/auth.json')
    #LOCATION_SCHEMA = utils.load_json(PATH+'/DataStructure/locations.json')
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
    next_date = datetime(int(date[0]), int(date[1]), int(date[2])) + timedelta(days=1)
    day_filters['date_ranges'][0]['first_day'] = str(next_date).split(' ')[0]
    day_filters['date_ranges'][0]['last_day'] = str(next_date).split(' ')[0]



def metrics_extractor_day(pids, auth):
    metrics_schema = utils.load_json(PATH+'/DataStructure/metric.json')
    filters = utils.load_json(PATH+'/filters/day_filter.json')
    metrics = utils.load_json(PATH+'/filters/day_metrics.json')
    headers_day(day_filters=filters)
    singer.write_schema('metrics', metrics_schema, ["name"])
    for rid in pids:
        for metric in metrics:
            filters['locations'] = [rid]
            filters['metrics'] = metrics[metric]
            header = str(filters).replace('\'', '"')
            page = requests.post(HREF_POST, auth=(auth['Access Key'], auth['Secret Key']) \
                                 , data=header)
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
                            singer.write_record('metrics', data)
                    except KeyError:
                        LOGGER.info('No data for metrics %s', str(metrics[metric]))
    filters_file = open(PATH+'/filters/day_filter.json', 'w')
    filters_file.write(header)
    filters_file.close()

    #singer.write_state(header)

def start_load_day():
    #os.chdir('E:\\Projects\\Casper')
    auth = utils.load_json(PATH+'/auth.json')
    #location_schema = utils.load_json(PATH+'/DataStructure/locations.json')
    LOGGER.info("Staring schema defination.")
    location_gen = location_extractor(auth)
    records = []
    #singer.write_schema('locations', location_schema, ["id"])
    for record in location_gen:
        records.append(record)
        #singer.write_record('locations', record)
    pid = [i['parent_id'] for i in records]
    ids = [i['id'] for i in records]
    leaf_level_ids = [i for i in ids if i not in pid]
    if leaf_level_ids:
        metrics_extractor_day(leaf_level_ids, auth)

def main():
    if OPERATION_TYPE == 'day':
        LOGGER.info('Started data load for daily level metrics')
        start_load_day()

    elif OPERATION_TYPE == 'minute':
        LOGGER.info('Started data load for minutes level metrics')
        start_load_min()

if __name__ == "__main__":
    main()
