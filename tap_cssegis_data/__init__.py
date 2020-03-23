#!/usr/bin/env python3
import os
import json
import singer
import sys
import csv
import codecs
from datetime import datetime, timedelta
import urllib.request as request
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_cssegis_data import metrics as zendesk_metrics
from tap_cssegis_data.discover import discover_streams
from tap_cssegis_data.streams import STREAMS

REQUIRED_CONFIG_KEYS = ["start_date", "repository", "branch", "path"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    LOGGER.info("Starting schema")
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)#Schema.from_dict(json.load(file))
    LOGGER.info("Finished schema")
    return schemas


def do_discover():
    LOGGER.info("Starting discover")
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append({'stream': stream_id, 'tap_stream_id': stream_id, 'schema': schema, 'metadata': None})

    LOGGER.info("Finshed discover")
    return streams


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog['streams']:
        LOGGER.info("Syncing stream:" + stream['tap_stream_id'])
        us_state_codes_path = get_abs_path('codes') + '/us_state_codes.csv'
        with open(us_state_codes_path) as us_codes:
            reader = csv.reader(us_codes)
            us_state_codes = {rows[0]: rows[1] for rows in reader}

        last_date = None
        current_date = None
        if len(state) > 0:
            if 'daily' in stream['tap_stream_id']:
                last_date = datetime.strptime(state['bookmarks'][stream['tap_stream_id']]['last_date'],'%m-%d-%Y')
                current_date = (datetime.now() - timedelta(1)).strftime('%m-%d-%Y')
            else:
                last_date = datetime.strptime(state['bookmarks'][stream['tap_stream_id']]['last_date'],'%-m/%-d/%y')
                current_date = (datetime.now() - timedelta(1)).strftime('%-m/%-d/%y')
        else:
            if 'daily' in stream['tap_stream_id']:
                last_date = datetime.strptime('01-22-2020', '%m-%d-%Y').date()
                current_date = (datetime.now() - timedelta(1)).date()
            else:
                last_date = datetime.strptime('01-22-2020', '%m-%d-%Y').date()
                current_date = (datetime.now() - timedelta(1)).date()

        try:
            bookmark_column = stream['schema']['replication_key']
        except:
            print('replication_key does not exist for ' + stream['tap_stream_id'])
            bookmark_column = None

        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value
        #stream['key_properties'].append('id')
        singer.write_schema(
            stream_name=stream['tap_stream_id'],
            schema=stream['schema'],
            key_properties=stream['schema']['key_properties'],
        )
        timeseries = ('time_series_19-covid-Confirmed.csv','time_series_19-covid-Deaths.csv','time_series_19-covid-Recovered.csv')



        while last_date <= current_date:
            if 'daily' in stream['tap_stream_id']:
                daily = request.urlopen('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'+last_date.strftime('%m-%d-%Y')+'.csv')
            else:
                if 'confirmed' in stream['tap_stream_id']:
                    daily = request.urlopen('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv')
                elif 'deaths' in stream['tap_stream_id']:
                    daily = request.urlopen('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv')
                elif 'recovered' in stream['tap_stream_id']:
                    daily = request.urlopen('https://raw.githubusercontent.com/CSSEGISandData/COVID-19//master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv')

            tap_data = csv.reader(codecs.iterdecode(daily,'UTF-8'))
            max_bookmark = None
            isHeader = 0
            for row in tap_data:
                if 'daily' in stream['tap_stream_id']:
                    if isHeader > 0:
                        data_row = {}
                        data_row['Source'] = stream['tap_stream_id']
                        data_row['Description'] = 'Daily report for ' + last_date.strftime('%m-%d-%Y')
                        data_row['Is_A_Cruise'] = isCruise(row[0])
                        if 'US' in row[1]:
                            row[1] = 'United States'
                        if '*' in row[1]:
                            row[1] = row[1].replace('*', '')
                        if ',' in row[1]:
                            tv_country = row[1].split(', ')
                            row[1] = tv_country[1].replace('"', '') + ' ' + tv_country[0].replace('"', '')
                        if ',' in row[0] and row[1] == 'United States' and 'Princess' not in row[0]:
                            tv_state = row[0].replace('"','').split(', ')
                            if 'County' in tv_state[0]:
                                tv_state[0] = tv_state[0].replace(' County', '')
                            data_row['County'] = tv_state[0]
                            try:
                                data_row['State'] = (us_state_codes[tv_state[1].strip()])
                            except:
                                data_row['State'] = tv_state[1]
                        else:
                            data_row['County'] = ''
                            data_row['State'] = ''

                        data_row['Country'] = row[1]
                        data_row['Last_Date'] = row[2]

                        try:
                            data_row['Confirmed'] = int(row[3])
                        except:
                            data_row['Confirmed'] = 0
                        try:
                            data_row['Deaths'] = int(row[4])
                        except:
                            data_row['Deaths'] = 0
                        try:
                            data_row['Recovered'] = int(row[5])
                        except:
                            data_row['Recovered'] = 0
                        try:
                            data_row['Latitude'] = float(row[6])
                        except:
                            data_row['Latitude'] = 0.0

                        try:
                            data_row['Longitude'] = float(row[7])
                        except:
                            data_row['Longitude'] = 0.0

                        # write one or more rows to the stream:
                        singer.write_records(stream['tap_stream_id'], [data_row])
                else:
                    if isHeader == 0:
                        idx = getDateIndex(last_date, row)
                    else:
                        data_row = {}
                        data_row['Source'] = stream['tap_stream_id']
                        data_row['Description'] = 'Daily report for ' + last_date.strftime('%m-%d-%Y')
                        data_row['Is_A_Cruise'] = isCruise(row[0])
                        if 'US' in row[1]:
                            row[1] = 'United States'
                        if '*' in row[1]:
                            row[1] = row[1].replace('*', '')
                        if ',' in row[1]:
                            tv_country = row[1].split(', ')
                            row[1] = tv_country[1].replace('"', '') + ' ' + tv_country[0].replace('"', '')
                        if ',' in row[0] and row[1] == 'United States' and 'Princess' not in row[0]:
                            tv_state = row[0].replace('"','').split(', ')
                            if 'County' in tv_state[0]:
                                tv_state[0] = tv_state[0].replace(' County', '')
                            data_row['County'] = tv_state[0]
                            try:
                                data_row['State'] = (us_state_codes[tv_state[1].strip()])
                            except:
                                data_row['State'] = tv_state[1]
                        else:
                            data_row['County'] = ''
                            data_row['State'] = ''

                        if 'confirmed' in stream['tap_stream_id']:
                            data_row['Confirmed'] = int(row[idx])
                        elif 'deaths' in stream['tap_stream_id']:
                            data_row['Deaths'] = int(row[idx])
                        elif 'recovered' in stream['tap_stream_id']:
                            data_row['Recovered'] = int(row[idx])

                        try:
                            data_row['Latitude'] = float(row[2])
                        except:
                            data_row['Latitude'] = 0.0

                        try:
                            data_row['Longitude'] = float(row[3])
                        except:
                            data_row['Longitude'] = 0.0

                        singer.write_records(stream['tap_stream_id'], [data_row])


                isHeader = 1

            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({'bookmarks': {stream['tap_stream_id']: {'last_date': last_date.strftime('%m-%d-%Y')}}})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])





            last_date = last_date + timedelta(days=1)
    return

def getDateIndex(last_date, row):
    i = 4
    while i < len(row):
        try:
            d = datetime.strptime(row[i], '%m/%d/%y').date()
        except:
            d = datetime.strptime('1/1/00', '%m/%d/%y').date()

        if (last_date == d):
            break
        i += 1

    return i


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = {"streams": do_discover()}
        json.dump(catalog, sys.stdout, indent=2)
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = {"streams": do_discover()}
            json.dump(catalog, sys.stdout, indent=2)
        sync(args.config, args.state, catalog)

def isCruise(data):
    if 'Princess' in data:
        return 1
    else:
        return 0


if __name__ == "__main__":
    main()
