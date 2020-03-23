import os
import json
import datetime
import pytz
#import zenpy
#from zenpy.lib.exception import RecordNotFoundException
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point


LOGGER = singer.get_logger()
KEY_PROPERTIES = ['id']

CUSTOM_TYPES = {
    'text': 'string',
    'textarea': 'string',
    'date': 'string',
    'regexp': 'string',
    'dropdown': 'string',
    'integer': 'integer',
    'decimal': 'number',
    'checkbox': 'boolean',
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None

    def __init__(self, client=None):
        self.client = client

    def get_bookmark(self, state):
        return utils.strptime_with_tz(singer.get_bookmark(state, self.name, self.replication_key))

    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return self._add_custom_fields(schema)

    def _add_custom_fields(self, schema): # pylint: disable=no-self-use
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None


class Daily(Stream):
    name = "cssegis-daily"
    replication_method = "FULL_TABLE"

    def sync(self, state): # pylint: disable=unused-argument
        for policy in self.client.sla_policies():
            yield (self.stream, policy)

class TimeSeriesConfirmed(Stream):
    name = "cssegis-timeseries-confirmed"
    replication_method = "INCREMENTAL"
    replication_key = "date"


class TimeSeriesDeaths(Stream):
    name = "cssegis-timeseries-deaths"
    replication_method = "INCREMENTAL"
    replication_key = "date"


class TimeSeriesRecovered(Stream):
    name = "cssegis-timeseries-recovered"
    replication_method = "INCREMENTAL"
    replication_key = "date"


STREAMS = {
    "cssegis-daily": "Daily",
    "cssegis-timeseries-confirmed": "TimeSeriesConfirmed",
    "cssegis-timeseries-deaths": "TimeSeriesDeaths",
    "cssegis-timeseries-recovered": "TimeSeriesRecovered"
}
