import os
import json
import singer
from tap_cssegis_data.streams import STREAMS
from tap_cssegis_data import metrics as zendesk_metrics

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)



def discover_streams():
    streams = []

    for s in STREAMS.values():
        s = s()
        schema = singer.resolve_schema_references(s.load_schema())
        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': schema, 'metadata': s.load_metadata()})
    return streams