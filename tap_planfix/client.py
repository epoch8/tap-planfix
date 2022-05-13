"""REST client handling, including PlanfixStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
import copy
import os

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BearerTokenAuthenticator


class PlanfixStream(RESTStream):
    """Planfix stream class."""

    url_base = os.environ.get("PLANFIX_URL", "https://youtravel.planfix.ru/rest")
    planfix_token = os.environ.get("PLANFIX_TOKEN", "d5173cb610825049967f700c2347a487")
    rest_method = "POST"
    payload_offset = 0

    records_jsonpath = "$.contacts[*]"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.planfix_token
        )

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        results = response.json()

        if not results or not results["contacts"]:
            return None

        return response.url

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            finished = not next_page_token

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        payload = {
          "offset": self.payload_offset,
          "pageSize": 100,
          "fields": "id,name,lastname,email,phones"
        }

        self.payload_offset += 100
        return payload

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

