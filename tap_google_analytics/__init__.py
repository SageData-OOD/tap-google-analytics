import json
import re

import backoff
import requests
import sys
import threading

import singer
from singer import utils, metadata
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from singer.transform import transform
from datetime import datetime, timedelta

ATTRIBUTION_WINDOW_DAYS = 14
LOGGER = singer.get_logger()
ROWS_LIMIT = 100000
HOST = "https://analyticsdata.googleapis.com/v1beta"
ENDPOINT = {
    "google_analytics_v4": "/{property}:runReport",
    "metadata": "/{property}/metadata"
}

REQUIRED_CONFIG_KEYS = ["start_date", "metrics", "dimensions", "property_id", "account_id",
                        "client_id", "client_secret", "token_uri", "refresh_token"]


class GAdsRateLimitError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


def get_key_properties(stream_name, dimensions):
    """
    Returns list of primary key columns as per report.
    It will dynamically generate list of keys based on selected dimensions.
    """

    default_key_properties = {
        "google_analytics_v4": ["start_date", "account_id", "property_id"]
    }

    return default_key_properties.get(stream_name, []) + [camel_to_snake_case(d) for d in dimensions]


def create_metadata_for_report(schema, key_properties):
    # doing ["selected": True] as we are creating metadata from already selected fields.

    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available",
                                             "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": "start_date",
                                             "table-key-properties": key_properties,
                                             "selected": True}}]

    for key in schema.properties:
        if "object" in schema.properties.get(key).type:
            for prop in schema.properties.get(key).properties:
                inclusion = "automatic" if prop in key_properties else "available"
                mdata.extend([{
                    "breadcrumb": ["properties", key, "properties", prop],
                    "metadata": {"inclusion": inclusion, "selected": True}
                }])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key],
                          "metadata": {"inclusion": inclusion, "selected": True}
                          })

    return mdata


def requests_session(session=None):
    """
    Creates or configures an HTTP session to use retries
    Returns:
        The configured HTTP session object
    """
    session = session or requests.Session()
    return session


def refresh_token(config):
    headers = {"Content-Type": "application/json"}
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token']
    }
    url = config['token_uri']
    response = requests.post(url, headers=headers, data=json.dumps(data),
                             auth=(config["client_id"], config['client_secret']))
    return response.json()


def refresh_access_token_if_expired(config):
    # if [expires_at not exist] or if [exist and less then current time] then it will update the token
    if config.get('expires_at') is None or config.get('expires_at') < datetime.utcnow():
        res = refresh_token(config)
        config["access_token"] = res["access_token"]
        config["expires_at"] = datetime.utcnow(
        ) + timedelta(seconds=res["expires_in"])
        return True
    return False


@backoff.on_exception(backoff.expo, GAdsRateLimitError, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def request_data(config, headers, endpoint, req_type, session=None, payload=None):
    url = HOST + endpoint.format(property=config["property_id"])
    session = requests_session(session)
    if refresh_access_token_if_expired(config) or not headers.get("access_token"):
        headers["Authorization"] = f'Bearer {config["access_token"]}'

    if req_type == "GET":
        response = session.get(url, headers=headers)
    else:
        response = session.post(url, data=json.dumps(payload), headers=headers)
    res = response.json()
    if response.status_code == 429:
        raise GAdsRateLimitError(response.text)
    elif response.status_code != 200:
        raise Exception(response.text)
    return res


def generate_request_payload(config):
    dimensions = [{"name": dimension} for dimension in config["dimensions"]]
    metrics = [{"name": metric} for metric in config["metrics"]]

    payload = {
        "dimensions": dimensions,
        "metrics": metrics,
        "limit": ROWS_LIMIT
    }

    if config.get("dimension_filter"):
        payload["dimensionFilter"] = json.loads(config["dimension_filter"])
    if config.get("metric_filter"):
        payload["metricFilter"] = json.loads(config["metric_filter"])

    return payload


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        bookmark_column = "last_sync_date"
        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties
        )

        if state.get("bookmarks", {}).get(stream.tap_stream_id):
            start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column)

            # offset back start date for the bookmark because of delayed attribution
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            min_start_date = datetime.utcnow().date() - timedelta(days=ATTRIBUTION_WINDOW_DAYS)

            if start_date_dt > min_start_date:
                start_date = str(min_start_date)
        else:
            start_date = config["start_date"]

        payload = generate_request_payload(config)
        headers = {"Content-Type": "application/json"}
        session = requests_session()
        while True:
            offset = 0
            row_count = 1  # bypass condition for first request
            payload["dateRanges"] = [{"startDate": start_date, "endDate": start_date}]

            while offset < row_count:
                data = request_data(config, headers, ENDPOINT[stream.tap_stream_id], "POST", session=session, payload=payload)
                field_headers = [
                    camel_to_snake_case(d["name"])
                    for d in
                    data["dimensionHeaders"] + data['metricHeaders']
                ]

                rows = data.get("rows", [])
                with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                    for row in rows:
                        values = [
                            v["value"]
                            for v in
                            row.get("dimensionValues", []) + row.get("metricValues", [])
                        ]
                        record = dict(zip(field_headers, values))
                        record["start_date"] = start_date
                        record["account_id"] = config["account_id"]
                        record["property_id"] = config["property_id"].split("/")[-1]

                        transformed_data = transform(record, schema, metadata=mdata)

                        # write one or more rows to the stream:
                        singer.write_records(stream.tap_stream_id, [transformed_data])
                        counter.increment()

                row_count = data.get("rowCount", 0)
                offset += len(rows)

            state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, start_date)
            singer.write_state(state)

            start_date = str(datetime.strptime(start_date, '%Y-%m-%d').date() + timedelta(days=1))
            
            if start_date > str(datetime.utcnow().date()):
                break


pattern_one = re.compile('(.)([A-Z][a-z]+)')
pattern_two = re.compile('__([A-Z])')
pattern_three = re.compile('([a-z0-9])([A-Z])')


def camel_to_snake_case(name):
    # camel2_camel2_case --> camel2_camel2_case
    # getHTTPResponseCode --> get_http_response_code
    # HTTPResponseCodeXYZ --> http_response_code_xyz
    name = name.replace(":", "_")   # customEvent:meta_data -> customEvent_meta_data
    name = name.replace(" ", "_")
    name = pattern_one.sub(r'\1_\2', name)
    name = pattern_two.sub(r'_\1', name)
    return pattern_three.sub(r'\1_\2', name).lower()


def get_schema_specific_data_type(_type):
    metric_type = {
        "METRIC_TYPE_UNSPECIFIED": "string",
        "TYPE_INTEGER": "integer",
        "TYPE_FLOAT": "number",
        "TYPE_SECONDS": "number",
        "TYPE_MILLISECONDS": "number",
        "TYPE_MINUTES": "number",
        "TYPE_HOURS": "number",
        "TYPE_STANDARD": "number",
        "TYPE_CURRENCY": "number",
        "TYPE_FEET": "number",
        "TYPE_MILES": "number",
        "TYPE_METERS": "number",
        "TYPE_KILOMETERS": "number"
    }
    return ["null", metric_type.get(_type, "string")]


def generate_schema(config):
    headers = {'Content-Type': 'application/json'}
    endpoint = ENDPOINT["metadata"].format(property=config["property_id"])
    data = request_data(config, headers, endpoint, "GET")

    all_dimensions = {d["apiName"]: {"type": ["null", "string"]} for d in data.get("dimensions", [])}
    all_metrics = {m["apiName"]: {"type": get_schema_specific_data_type(m["type"])} for m in data.get("metrics", [])}

    properties = {
        "start_date": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "account_id": {"type": ["null", "string"]},
        "property_id": {"type": ["null", "string"]},
        **{camel_to_snake_case(dia): all_dimensions[dia] for dia in config["dimensions"]},  # selected dimensions
        **{camel_to_snake_case(met): all_metrics[met] for met in config["metrics"]}        # selected metrics
    }
    return Schema.from_dict({"type": ["null", "object"], "properties": properties})


def create_catalog_based_on_selected_fields(config):
    stream_name = "google_analytics_v4"
    dimensions = config["dimensions"]
    schema = generate_schema(config)
    key_properties = get_key_properties(stream_name, dimensions)
    streams = [
        CatalogEntry(
            tap_stream_id=stream_name,
            stream=stream_name,
            schema=schema,
            key_properties=key_properties,
            metadata=create_metadata_for_report(schema, key_properties)
        )
    ]
    return Catalog(streams)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # We skip discovery call as SageData uses frontend to call list metrics and dimensions
    # Dummy DiscoveryCall TO pass the arg parser
    if args.discover:
        json.dump({"streams": [{
            "tap_stream_id": "file_metadata",
            "key_properties": [],
            "schema": {},
            "stream": "file_metadata",
            "metadata": []
            }]}, sys.stdout, indent=2)

    # Otherwise, run in sync mode
    else:
        catalog = create_catalog_based_on_selected_fields(args.config)
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
