#!/usr/bin/env python3
import os
import backoff
import requests
import singer
from datetime import datetime
from singer import Transformer, utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


LOGGER = singer.get_logger()
SESSION = requests.Session()
REQUIRED_CONFIG_KEYS = ["access_token"]
BASE_API_URL = "https://www.wrike.com/api/v4/"
CONFIG = {}
STATE = {}


def get_access_token():
    return CONFIG.get('access_token')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    """ Load schema by name """
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        schemas['filename'] = Schema.from_dict(utils.load_json(filename))

    return schemas


def get_url(endpoint):
    """ Get endpoint URL """
    return BASE_API_URL + endpoint


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5, giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, factor=2)
@utils.ratelimit(100, 15)
def request(url, params=None):
    params = params or {}
    access_token = get_access_token()
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer " + access_token
    }
    req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()

    return resp.json()


def sync_endpoint(schema_name):
    """ Sync endpoint data """
    schema = load_schema(schema_name)
    singer.write_schema(schema_name, schema, ["id"])

    with Transformer() as transformer:
        url = get_url(schema_name)
        response = request(url)
        data = response['data']
        time_extracted = utils.now()

        for row in data:
            item = transformer.transform(row, schema)
            singer.write_record(schema_name, item, time_extracted=time_extracted)


def sync(catalog):
    """ Sync data from tap source """
    for stream in catalog.get_selected_streams(STATE):
        LOGGER.info("Syncing stream: " + stream.tap_stream_id)
        sync_endpoint(stream.tap_stream_id)
        singer.write_state({"last_updated_at": str(datetime.now().isoformat()), "stream": stream.tap_stream_id})
    return


def discover():
    raw_schemas = load_schemas()
    streams = []

    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None
            )
        )

    return Catalog(streams)

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    STATE.update(args.state)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()

    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(catalog)


if __name__ == "__main__":
    main()