"""Stream class for tap-facebook-pages."""
import time as t
import datetime
import re
import sys
import copy
import json
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, cast

import pendulum
from singer_sdk.streams import RESTStream
import backoff
import functools

import singer
from singer import metadata

import urllib.parse
import requests
import logging

from tap_facebook_pages.client import FacebookPagesStream

logger = logging.getLogger("tap-facebook-pages")
logger_handler = logging.StreamHandler(stream=sys.stderr)
logger.addHandler(logger_handler)
logger.setLevel("INFO")
logger_handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))

NEXT_FACEBOOK_PAGE = "NEXT_FACEBOOK_PAGE"
MAX_RETRY = 5
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class Page(FacebookPagesStream):
    name = "page"
    tap_stream_id = "page"
    path = "/{id}"
    primary_keys = ["id"]
    replication_key = None
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "page.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = {}
        fields = ','.join(self.config['columns']) if 'columns' in self.config else ','.join(
            self.schema["properties"].keys())
        params.update({"fields": fields})
        return params

    def post_process(self, row: dict, stream_or_partition_state: dict) -> dict:
        return row


class Posts(FacebookPagesStream):
    name = "posts"
    tap_stream_id = "posts"
    path = "/{id}/posts"
    primary_keys = ["id"]
    replication_key = "created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "posts.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = {}
        if next_page_token:
            return urllib.parse.parse_qs(urllib.parse.urlparse(next_page_token).query)

        starting_datetime = self.get_starting_timestamp(partition)
        if starting_datetime:
            params["since"] = int(starting_datetime.timestamp())

        params["limit"] = 100
        fields = ','.join(self.config.get('columns', self.schema["properties"].keys()))
        params["fields"] = fields
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            row["id"] = self.id
            yield row


class PostTaggedProfile(FacebookPagesStream):
    name = "post_tagged_profile"
    tap_stream_id = "post_tagged_profile"
    path = "/posts"
    primary_keys = ["id"]
    replication_key = "post_created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "post_tagged_profile.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        if next_page_token is None or isinstance(next_page_token, str):
            params = super().get_url_params(partition, next_page_token)
        else:
            params = next_page_token
        time = int(t.time()) + 86400  # add one day to the last until time
        day = int(datetime.timedelta(1).total_seconds())
        if not next_page_token:
            # check difference between start date and state date. Update since if necessary
            state = self.get_stream_or_partition_state({'id': self.id})
            if 'progress_markers' in state and state['progress_markers']:
                state_date = state['progress_markers']['replication_key_value']
                since = int(cast(datetime.datetime, pendulum.parse(state_date)).timestamp())
                if since > params['since']:
                    params['since'] = since

            until = params['since'] + 7689600  # 8035200
            params.update({"until": until if until <= time else time - day})
        else:
            until = params['until'][0]
            since = params['since'][0]
            difference = (int(until) - int(since))
            if difference > 8035200:
                params['until'][0] = int(until) - (difference - 8035200)
            if int(until) > time:
                params['until'][0] = str(time - day)

        params.update({"fields": "id,created_time,to"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            parent_info = {
                "id": self.id,
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            if "to" in row:
                for attachment in row["to"]["data"]:
                    attachment.update(parent_info)
                    yield attachment


class PostAttachments(FacebookPagesStream):
    name = "post_attachments"
    tap_stream_id = "post_attachments"
    path = "/posts"
    primary_keys = ["id"]
    replication_key = "post_created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "post_attachments.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        if next_page_token is None or isinstance(next_page_token, str):
            params = super().get_url_params(partition, next_page_token)
        else:
            params = next_page_token
        time = int(t.time()) + 86400  # add one day to the last until time
        day = int(datetime.timedelta(1).total_seconds())
        if not next_page_token:
            # check difference between start date and state date. Update since if necessary
            state = self.get_stream_or_partition_state({'id': self.id})
            if 'progress_markers' in state and state['progress_markers']:
                state_date = state['progress_markers']['replication_key_value']
                since = int(cast(datetime.datetime, pendulum.parse(state_date)).timestamp())
                if since > params['since']:
                    params['since'] = since

            until = params['since'] + 7689600  # 8035200
            params.update({"until": until if until <= time else time - day})
        else:
            until = params['until'][0]
            since = params['since'][0]
            difference = (int(until) - int(since))
            if difference > 8035200:
                params['until'][0] = int(until) - (difference - 8035200)
            if int(until) > time:
                params['until'][0] = str(time - day)

        params.update({"fields": "id,created_time,attachments"})

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            parent_info = {
                "id": self.id,
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            if "attachments" in row:
                for attachment in row["attachments"]["data"]:
                    if "subattachments" in attachment:
                        for sub_attachment in attachment["subattachments"]["data"]:
                            sub_attachment.update(parent_info)
                            yield sub_attachment
                        attachment.pop("subattachments")
                    attachment.update(parent_info)
                    yield attachment


class PageInsights(FacebookPagesStream):
    name = None
    tap_stream_id = None
    path = "/insights"
    primary_keys = ["id"]
    replication_key = None
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "page_insights.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        if next_page_token is None or isinstance(next_page_token, str):
            params = super().get_url_params(partition, next_page_token)
        else:
            params = next_page_token
        time = int(t.time()) + 86400  # add one day to the last until time
        day = int(datetime.timedelta(1).total_seconds())
        if not next_page_token:
            # check difference between start date and state date. Update since if necessary
            state = self.get_stream_or_partition_state({'id': self.id})
            if 'progress_markers' in state and state['progress_markers']:
                state_date = state['progress_markers']['replication_key_value']
                since = int(cast(datetime.datetime, pendulum.parse(state_date)).timestamp())
                if since > params['since']:
                    params['since'] = since

            until = params['since'] + 7689600  # 8035200
            params.update({"until": until if until <= time else time - day})
        else:
            until = params['until'][0]
            since = params['since'][0]
            difference = (int(until) - int(since))
            if difference > 8035200:
                params['until'][0] = int(until) - (difference - 8035200)
            if int(until) > time:
                params['until'][0] = str(time - day)

        params.update({"metric": ",".join(self.metrics)})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row["title"],
                "id": row["id"],
            }
            if "values" in row:
                for values in row["values"]:
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            item = {
                                "context": key,
                                "value": value,
                                "end_time": values["end_time"]
                            }
                            item.update(base_item)
                            yield item
                    else:
                        values.update(base_item)
                        yield values


class PostInsights(FacebookPagesStream):
    name = ""
    tap_stream_id = ""
    # use published_posts instead of feed, as the last one is problematic endpoint
    # path = "/feed"
    path = "/published_posts"
    primary_keys = ["id"]
    replication_key = "post_created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "post_insights.json"

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        if next_page_token is None or isinstance(next_page_token, str):
            params = super().get_url_params(partition, next_page_token)
        else:
            params = next_page_token
        time = int(t.time()) + 86400  # add one day to the last until time
        day = int(datetime.timedelta(1).total_seconds())
        if not next_page_token:
            # check difference between start date and state date. Update since if necessary
            state = self.get_stream_or_partition_state({'id': self.id})
            if 'progress_markers' in state and state['progress_markers']:
                state_date = state['progress_markers']['replication_key_value']
                since = int(cast(datetime.datetime, pendulum.parse(state_date)).timestamp())
                if since > params['since']:
                    params['since'] = since

            until = params['since'] + 7689600  # 8035200
            params.update({"until": until if until <= time else time - day})
        else:
            until = params['until'][0]
            since = params['since'][0]
            difference = (int(until) - int(since))
            if difference > 8035200:
                params['until'][0] = int(until) - (difference - 8035200)
            if int(until) > time:
                params['until'][0] = str(time - day)

        params.update({"fields": "id,created_time,insights.metric(" + ",".join(self.metrics) + ")"})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            for insights in row["insights"]["data"]:
                base_item = {
                    "post_id": row["id"],
                    "id": self.id,
                    "post_created_time": row["created_time"],
                    "name": insights["name"],
                    "period": insights["period"],
                    "title": insights["title"],
                    "description": insights["description"],
                    "id": insights["id"],
                }
                if "values" in insights:
                    for values in insights["values"]:
                        if isinstance(values["value"], dict):
                            for key, value in values["value"].items():
                                item = {
                                    "context": key,
                                    "value": value,
                                }
                                item.update(base_item)
                                yield item
                        else:
                            values.update(base_item)
                            yield values
