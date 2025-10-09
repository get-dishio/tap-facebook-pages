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

    def sync(self):
        """Override sync to manually emit records, bypassing SDK orchestration."""
        import singer
        partitions = getattr(self, "_partitions", []) or []
        for partition in partitions:
            if not partition.get("id"):
                logger.info(f"SKIP: Page.sync skipping partition with no id: {partition}")
                continue
            rows_yielded = 0
            for row in FacebookPagesStream.request_records(self, partition):
                singer.write_record(self.name, row)
                rows_yielded += 1

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        partitions = getattr(self, "_partitions", []) or []
        for partition in partitions:
            if not partition.get("id"):
                logger.info(f"SKIP: Page.get_records skipping partition with no id: {partition}")
                continue
            # Explicitly call the parent class's request_records to avoid MRO issues
            for row in FacebookPagesStream.request_records(self, partition):
                yield row

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        params = {}
        fields = ','.join(self.config['columns']) if 'columns' in self.config else ','.join(
            self.schema["properties"].keys())
        params.update({"fields": fields})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        data = response.json()
        # Facebook /{id} returns a single object, not a list
        yield data

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

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        # Only yield records for partitions with a valid id
        partitions = getattr(self, "partitions", []) or []
        for partition in partitions:
            if not partition.get("id"):
                logger.info(f"SKIP: Posts.get_records skipping partition with no id: {partition}")
                continue
            yield from self.request_records(partition)

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
            yield row


class PostTaggedProfile(FacebookPagesStream):
    name = "post_tagged_profile"
    tap_stream_id = "post_tagged_profile"
    path = "/{id}/posts"
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
            partition_id = partition.get("id") if partition else None
            state = self.get_stream_or_partition_state({'id': partition_id})
            if 'progress_markers' in state and state['progress_markers']:
                state_date = state['progress_markers']['replication_key_value']
                since = int(cast(datetime.datetime, pendulum.parse(state_date)).timestamp())
                if 'since' in params:
                    if since > params['since']:
                        params['since'] = since
                else:
                    params['since'] = since

            if 'since' in params:
                until = params['since'] + 7689600  # 8035200
                params.update({"until": until if until <= time else time - day})
        else:
            if 'until' in params and 'since' in params:
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
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            if "to" in row:
                for tag in row["to"]["data"]:
                    out = dict(tag)
                    out.update(parent_info)
                    yield out


class PostAttachments(FacebookPagesStream):
    name = "post_attachments"
    tap_stream_id = "post_attachments"
    path = "/{id}/posts"
    primary_keys = ["attachment_id"]
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
            partition_id = partition.get("id") if partition else None
            state = self.get_stream_or_partition_state({'id': partition_id})
            if 'progress_markers' in state and state['progress_markers']:
                state_date = state['progress_markers']['replication_key_value']
                since = int(cast(datetime.datetime, pendulum.parse(state_date)).timestamp())
                if since > params['since']:
                    params['since'] = since

            if 'since' in params:
                until = params['since'] + 7689600  # 8035200
                params.update({"until": until if until <= time else time - day})
        else:
            if 'until' in params and 'since' in params:
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
            parent = {
                "post_id": row["id"],
                "post_created_time": row["created_time"]
            }
            attachment_index = 0
            if "attachments" in row:
                for att in row["attachments"]["data"]:
                    if "subattachments" in att:
                        for sub in att["subattachments"]["data"]:
                            out = dict(sub)
                            out.update(parent)
                            out["attachment_id"] = f"{row['id']}_{row['created_time']}_{attachment_index}"
                            attachment_index += 1
                            yield out
                        att = dict(att)
                        att.pop("subattachments", None)
                    out = dict(att)
                    out.update(parent)
                    out["attachment_id"] = f"{row['id']}_{row['created_time']}_{attachment_index}"
                    attachment_index += 1
                    yield out


class PageInsights(FacebookPagesStream):
    name = None
    tap_stream_id = None
    path = "/{id}/insights"
    primary_keys = ["id"]
    replication_key = None
    forced_replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "page_insights.json"

    def get_partitions(self) -> Optional[Iterable[dict]]:
        # Allow explicit partition override for Singer SDK
        if hasattr(self, "_partitions") and self._partitions is not None:
            return self._partitions
        return super().get_partitions()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        # Explicitly iterate over partitions to ensure partition is passed to get_url
        partitions = self.get_partitions() or [{}]
        for partition in partitions:
            logger.info(f"DEBUG: PageInsights.get_records using partition: {partition}")
            if not partition.get("id"):
                logger.info(f"SKIP: PageInsights.get_records skipping partition with no id: {partition}")
                continue
            yield from self.request_records(partition)

    def get_url_params(self, partition: Optional[dict], next_page_token: Optional[Any] = None) -> Dict[str, Any]:
        if next_page_token is None or isinstance(next_page_token, str):
            params = super().get_url_params(partition, next_page_token)
        else:
            params = next_page_token
        time = int(t.time()) + 86400  # add one day to the last until time
        day = int(datetime.timedelta(1).total_seconds())
        if not next_page_token:
            # check difference between start date and state date. Update since if necessary
            partition_id = partition.get("id") if partition else None
            state = self.get_stream_or_partition_state({'id': partition_id})
            if 'progress_markers' in state and state['progress_markers']:
                state_date = state['progress_markers']['replication_key_value']
                since = int(cast(datetime.datetime, pendulum.parse(state_date)).timestamp())
                if since > params['since']:
                    params['since'] = since

            if 'since' in params:
                until = params['since'] + 7689600  # 8035200
                params.update({"until": until if until <= time else time - day})
        else:
            if 'until' in params and 'since' in params:
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
    # This stream now fetches post insights per post, not in bulk
    path = "/{id}/posts"  # Only used to fetch posts, not insights
    primary_keys = ["id"]
    replication_key = "post_created_time"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "post_insights.json"

    def get_partitions(self) -> Optional[Iterable[dict]]:
        # Allow explicit partition override for Singer SDK
        if hasattr(self, "_partitions") and self._partitions is not None:
            return self._partitions
        return super().get_partitions()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        # For each page, fetch posts, then fetch insights for each post
        import requests
        partitions = self.get_partitions() or [{}]
        for partition in partitions:
            page_id = partition.get("id")
            if not page_id:
                continue
            # 1. Fetch posts for this page
            url_base = f"https://graph.facebook.com/{self.config['api_version']}"
            posts_url = f"{url_base}/{page_id}/posts"
            params = {
                "access_token": self.config["access_token"],
                "fields": "id,created_time",
                "limit": 100
            }
            has_next = True
            next_url = posts_url
            while has_next and next_url:
                resp = requests.get(next_url, params=params)
                data = resp.json()
                posts = data.get("data", [])
                for post in posts:
                    post_id = post.get("id")
                    created_time = post.get("created_time")
                    if not post_id:
                        continue
                    # 2. Fetch insights for this post
                    insights_url = f"{url_base}/{post_id}/insights"
                    insights_params = {
                        "access_token": self.config["access_token"],
                        "metric": ",".join(self.metrics)
                    }
                    insights_resp = requests.get(insights_url, params=insights_params)
                    insights_data = insights_resp.json()
                    for ins in insights_data.get("data", []):
                        base = {
                            "post_id": post_id,
                            "post_created_time": created_time,
                            "name": ins.get("name"),
                            "period": ins.get("period"),
                            "title": ins.get("title"),
                            "description": ins.get("description"),
                            "id": ins.get("id"),
                        }
                        if "values" in ins:
                            for v in ins["values"]:
                                if isinstance(v.get("value"), dict):
                                    for k, val in v["value"].items():
                                        rec = {"context": k, "value": val}
                                        rec.update(base)
                                        yield rec
                                else:
                                    rec = dict(v)
                                    rec.update(base)
                                    yield rec
                # Pagination for posts
                paging = data.get("paging", {})
                next_url = paging.get("next")
                has_next = bool(next_url)
                # After first page, don't send params again (next_url already has them)
                params = {}
