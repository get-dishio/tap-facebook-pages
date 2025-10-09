"""facebook-pages tap class."""
import json
import logging
from pathlib import PurePath
from typing import List, Union
import requests
import singer
from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
)

from tap_facebook_pages.insights import INSIGHT_STREAMS
from tap_facebook_pages.streams import (
    Page, Posts, PostAttachments, PostTaggedProfile
)

PLUGIN_NAME = "tap-facebook-pages"

STREAM_TYPES = [
    Page,
    Posts,
    PostAttachments,
    PostTaggedProfile,
]

ACCOUNTS_URL = "https://graph.facebook.com/{version}/{user_id}/accounts"
ME_URL = "https://graph.facebook.com/{version}/me"
BASE_URL = "https://graph.facebook.com/{version}/{id}"

session = requests.Session()


class TapFacebookPages(Tap):
    name = PLUGIN_NAME

    _logger = singer.get_logger("FacebookPages")

    config_jsonschema = PropertiesList(
        Property("access_token", StringType, required=True),
        Property(
            "page_ids",
            ArrayType(
                PropertiesList(
                    Property("id", StringType, required=True),
                    Property("name", StringType)
                ).to_type()
            )
        ),
        Property("start_date", DateTimeType, required=True),
        Property("api_version", StringType, default="v12.0"),
    ).to_dict()

    def __init__(self, config: Union[PurePath, str, dict, None] = None,
                 catalog: Union[PurePath, str, dict, None] = None, state: Union[PurePath, str, dict, None] = None,
                 parse_env_config: bool = True) -> None:
        super().__init__(config, catalog, state, parse_env_config)
        # update page access tokens on sync
        page_objs = self.config['page_ids']
        self.page_ids = [p["id"] for p in page_objs]
        self.id_name_map = {p["id"]: p.get("name", "") for p in page_objs}

    def exchange_token(self, id: str, access_token: str):
        url = BASE_URL.format(
            version=self.config["api_version"],
            id=id
        )
        data = {
            'fields': 'access_token,name',
            'access_token': access_token
        }

        self.logger.info("Exchanging access token for page with id=" + id)
        response = session.get(url=url, params=data)
        response_data = json.loads(response.text)
        if response.status_code != 200:
            if response.status_code in (4, 17, 32, 613):
                self.logger.info("Retry to re-run after 1 hour ...")
            error_message = "Failed exchanging token: " + response_data["error"]["message"]
            self.logger.error(error_message)
            raise RuntimeError(
                error_message
            )
        self.logger.info("Successfully exchanged access token for page with id=" + id)
        return response_data['access_token']

    def get_pages_tokens(self, page_ids: list, access_token: str):
        params = {
            "access_token": access_token,
        }
        response = session.get(ME_URL.format(version=self.config["api_version"]), params=params)
        response_json = response.json()

        if response.status_code != 200:
            raise Exception(response_json["error"]["message"])

        # Get Access Tokens of Pages the user Manages
        params["fields"] = "name,access_token"
        params.update({"limit": 100})

        user_id = response_json["id"]
        next_page_cursor = True
        while next_page_cursor:
            response = session.get(ACCOUNTS_URL.format(version=self.config["api_version"], user_id=user_id), params=params)
            response_json = response.json()
            if response.status_code != 200:
                if response.status_code in (4, 17, 32, 613):
                    self.logger.info("Retry to re-run after 1 hour ...")
                raise Exception(response_json["error"]["message"])

            next_page_cursor = response_json.get("paging", {}).get("cursors", {}).get("after", False)
            params["after"] = next_page_cursor
            for pages in response_json["data"]:
                id = pages["id"]
                if page_ids and id not in page_ids:
                    continue

                self.logger.info("Get token for page '{}'".format(pages["name"]))
                self.access_tokens[id] = pages["access_token"]

    def discover_streams(self) -> List[Stream]:
        streams = []
        for stream_class in STREAM_TYPES:
            stream = stream_class(tap=self)
            streams.append(stream)

        for insight_stream in INSIGHT_STREAMS:
            stream = insight_stream["class"](tap=self, name=insight_stream["name"])
            stream.tap_stream_id = insight_stream["name"]
            stream.metrics = insight_stream["metrics"]
            streams.append(stream)
        return streams

    def load_streams(self) -> List[Stream]:
        # Get tokens now, at the beginning of the sync
        page_objs = self.config.get("page_ids")
        page_ids = [p["id"] for p in page_objs] if page_objs else []
        self.access_tokens = {}
        if page_ids and len(page_ids) == 1:
            self.access_tokens[page_ids[0]] = self.exchange_token(
                page_ids[0], self.config["access_token"]
            )
        else:
            self.get_pages_tokens(page_ids, self.config["access_token"])
            if not page_ids:
                self.logger.info(
                    "`page_ids` not provided. Using all accessible pages."
                )
                page_ids = list(self.access_tokens.keys())
                self.config["page_ids"] = [{"id": pid, "name": ""} for pid in page_ids]

        self.partitions = [{"id": x} for x in page_ids] if page_ids else []

        all_streams = self.discover_streams()

        # Filter based on catalog
        if self.input_catalog:
            selected_stream_ids = {
                s.tap_stream_id for s in self.input_catalog.streams if s.is_selected
            }
            loaded_streams = [
                s for s in all_streams if s.tap_stream_id in selected_stream_ids
            ]
        else:
            loaded_streams = all_streams

        # Attach tokens/partitions to the selected streams
        for stream in loaded_streams:
            stream.partitions = self.partitions
            stream.access_tokens = self.access_tokens
            self.logger.info(f"Loading stream: '{stream.tap_stream_id}'")

        return loaded_streams


# CLI Execution:

cli = TapFacebookPages.cli
