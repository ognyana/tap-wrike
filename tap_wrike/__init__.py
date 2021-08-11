#!/usr/bin/env python3
import os
import backoff
import requests
import singer
from singer import Transformer, utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import csv
import codecs
from datetime import datetime
from google.cloud import bigquery


LOGGER = singer.get_logger()
SESSION = requests.Session()
REQUIRED_CONFIG_KEYS = ["access_token", "bq_project", "bq_dataset"]
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


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
    factor=2
)
def request(url, params=None, as_csv=False):
    params = params or {}
    access_token = get_access_token()
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer " + access_token
    }

    if as_csv:
        download = requests.get(url, headers=headers)
        LOGGER.info("GET {}".format(url))
        csv_reader = csv.DictReader(codecs.iterdecode(download.content.splitlines(), 'utf-8'))

        return csv_reader

    req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()

    return resp.json()


def sync_csv_data(schema_name):
    """ Sync data from csv """
    url = get_url('data_export')
    response = request(url)
    resources = response['data'][0]['resources']
    item = list(filter(lambda resource: resource['name'] == schema_name, resources))
    item_csv_url = item[0]['url']

    bq_delete_table(schema_name)
    schema = load_schema(schema_name)
    singer.write_schema(schema_name, schema, ["id"])

    reader = request(item_csv_url, as_csv=True)
    with Transformer() as transformer:
        time_extracted = utils.now()

        for row in reader:
            item = transformer.transform(row, schema)
            singer.write_record(schema_name, item, time_extracted=time_extracted)

def bq_delete_table(schema_name):
    table_id = f"{CONFIG.get('bq_project')}.{CONFIG.get('bq_dataset')}.{schema_name}"

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.

    LOGGER.info("Deleted table '{}'.".format(table_id))

def sync(catalog):
    """ Sync data from tap source """
    for stream in catalog.get_selected_streams(STATE):
        LOGGER.info("Syncing stream: " + stream.tap_stream_id)
        sync_csv_data(stream.tap_stream_id)
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
