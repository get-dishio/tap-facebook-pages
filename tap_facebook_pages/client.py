"""Client stream class for tap-facebook-pages."""

import logging
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

logger = logging.getLogger("tap-facebook-pages")

class TooManyDataRequestedError(Exception):
    def __init__(self, msg=None, code=None):
        Exception.__init__(self, msg)
        self.code = code

def is_status_code_fn(blacklist=None, whitelist=None):
    def gen_fn(exc):
        status_code = getattr(exc, 'code', None)
        if status_code is None:
            return False
        status_code = getattr(exc, 'code', None)
        if status_code is None:
            return False

        if blacklist is not None and status_code not in blacklist:
            return True

        if whitelist is not None and status_code in whitelist:
            return True

        # Retry other errors up to the max
        return False

    return gen_fn


def retry_handler(details):
    """
        Customize retrying on Exception by updating until with reduced time
        (until - since) should be 90 days [7689600 -> 89 days + since, because since is included]
    """
    # Don't have to wait, just update 'until' param in prepared request
    details["wait"] = 0
    args = details["args"]
    message = "Too many data requested. "
    for arg in args:
        # decompose url in parts - get and update until param
        if hasattr(arg, "url"):
            url = args[args.index(arg)].url
            parsed_url = urllib.parse.urlparse(url)
            params = urllib.parse.parse_qs(parsed_url.query)

            # TODO: hide access token in url when an error message occurs
            since, until = params.get("since", False), params.get("until", False)
            if since:
                if not until:
                    until = [int(since[0]) + 7689600]

                days = int(((int(until[0]) - int(since[0])) / 86400) / 2) * 86400
                new_until = int(since[0]) + days
                logger.info("Updating time period into %s days", days / 86400)  # converted from seconds

                # update timeframe with until
                url = url.replace(params["until"][0], str(new_until))
                args[args.index(arg)].url = url

                # Update url for the next call
                details.update({
                    'args': args,
                })
                message += "Retrying with half period"

    logger.info(message + " -- Retry %s/%s", details['tries'], 5)


def error_handler(fnc):
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2,
    )
    @backoff.on_exception(
        backoff.expo,
        TooManyDataRequestedError,
        on_backoff=retry_handler,
        max_tries=5,
        giveup=is_status_code_fn(blacklist=[500]),
        jitter=None,
        max_value=60
    )
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)

    return wrapper


class FacebookPagesStream(RESTStream):
    """Stream class for facebook-pages streams."""

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """
        Ensure that records are always fetched with a valid partition context.
        If context is None, iterate over self._partitions (set in tap.py).
        """
        partitions = getattr(self, "_partitions", None)
        if context is None and partitions:
            for partition in partitions:
                yield from self.request_records(partition)
        else:
            yield from self.request_records(context)

    def get_stream_or_partition_state(self, partition: dict) -> dict:
        """Return the state for a given partition, or an empty dict if not found."""
        if hasattr(self, "state") and self.state:
            # The state structure may vary; this is a generic approach
            partition_id = partition.get("id")
            if partition_id and "bookmarks" in self.state:
                # Try to return the state for this stream and partition
                stream_state = self.state["bookmarks"].get(self.name, {})
                return stream_state.get(partition_id, {})
        return {}

    def get_url(self, partition: Optional[dict] = None, next_page_token: Optional[Any] = None) -> str:
        """Format the path with the partition/context dict and return the full URL."""
        context = partition or {}
        logger.info(f"DEBUG: get_url context for path formatting: {context}")
        try:
            path = self.path.format(**context)
        except KeyError as e:
            self.logger.critical(
                f"Could not format path '{self.path}' with context {context}. "
                f"Missing key: {e}"
            )
            raise
        return f"{self.url_base}{path}"

    @property
    def url_base(self) -> str:
        return f"https://graph.facebook.com/{self.config['api_version']}"

    def prepare_request(
        self,
        partition: Optional[dict],
        next_page_token: Optional[Any] = None
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        req = super().prepare_request(partition, next_page_token)

        # Use page-specific token if available, otherwise use main token
        token = self.config.get("access_token") # Default token
        if partition and "id" in partition:
            id = partition["id"]
            # access_tokens is a dict on the stream object, not in the config
            page_specific_token = getattr(self, "access_tokens", {}).get(id)
            if page_specific_token:
                token = page_specific_token

        if token:
            req.headers["Authorization"] = f"Bearer {token}"
        else:
            self.logger.warning("No access token found for request.")

        return req

    def request_records(self, partition: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records."""
        self.logger.info("Reading data for {}".format(partition and partition.get("id", False)))

        next_page_token: Any = None
        finished = False
        while not finished:
            prepared_request = self.prepare_request(
                partition, next_page_token=next_page_token
            )
            try:
                next_page_token = None
                resp = self._request_with_backoff(prepared_request)
                for row in self.parse_response(resp):
                    # Enrich rows with page_id if partition is present and stream expects it
                    if partition and "id" in partition:
                        pid = partition["id"]
                        # Add page_id for relevant streams
                        if getattr(self, "name", None) in {"posts", "post_tagged_profile", "post_attachments"}:
                            row.setdefault("page_id", pid)
                        # For insight streams, match by name prefix
                        elif getattr(self, "name", "").startswith("post_insight_") or getattr(self, "name", "").startswith("page_insight_"):
                            row.setdefault("page_id", pid)
                    yield row
                previous_token = copy.deepcopy(next_page_token)
                next_page_token = self.get_next_page_token(
                    response=resp, previous_token=previous_token
                )
                if next_page_token and next_page_token == previous_token:
                    raise RuntimeError(
                        f"Loop detected in pagination. "
                        f"Pagination token {next_page_token} is identical to prior token."
                    )
                finished = not next_page_token

                if next_page_token == 'True':
                    next_page_token = False

            except Exception as e:
                self.logger.warning(e)
                finished = not next_page_token

    @error_handler
    def _request_with_backoff(self, prepared_request) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if response.status_code in [401, 403]:
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            error = json.loads(response.content.decode("utf-8")).get("error", {})
            if error.get("code", False) == 1 and error.get("error_subcode", ) == 99:
                message = error.get("message", False) or "Too many data requested"
                raise TooManyDataRequestedError(message, code=500)

            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )
        logging.debug("Response received successfully.")
        return response
