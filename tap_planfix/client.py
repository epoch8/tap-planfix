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

    url_base = os.environ.get("PLANFIX_URL")
    planfix_token = os.environ.get("PLANFIX_TOKEN")
    rest_method = "POST"

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

        next_page_token = previous_token + 100 if previous_token else 100
        return next_page_token

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        payload = {
          "offset": next_page_token,
          "pageSize": 100,
          "fields": "id,name,lastname,email,phones"
        }

        return payload

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

