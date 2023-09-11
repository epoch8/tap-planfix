"""REST client handling, including PlanfixStream base class."""

import requests
from typing import Any, Dict, Optional, Union, List, Iterable
import pendulum
from datetime import date, datetime

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BearerTokenAuthenticator


class PlanfixStream(RESTStream):
    """Planfix stream class."""

    rest_method = "POST"
    PAGE_SIZE = 100

    @property
    def url_base(self) -> str:
        return self.config.get("planfix_url")  # type: ignore

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self, token=self.config.get("planfix_token")  # type: ignore
        )

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        results = response.json()

        if not results or not results["contacts"]:
            return None

        next_page_token = (
            previous_token + self.PAGE_SIZE if previous_token else self.PAGE_SIZE
        )
        return next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
