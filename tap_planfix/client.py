"""REST client handling, including PlanfixStream base class."""

import requests
from typing import Any, Dict, Optional, Union, List, Iterable
import pendulum
from datetime import date, datetime
from singer.schema import Schema

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BearerTokenAuthenticator


def extract_tag_name(string):
    start_index = string.index(".") + 1
    end_index = string.index("[", start_index)
    tag_name = string[start_index:end_index]
    return tag_name


class PlanfixStream(RESTStream):
    """Planfix stream class."""

    rest_method = "POST"
    PAGE_SIZE = 100
    fields_name_map = {}

    def __init__(
        self,
        tap: TapBaseClass,
        name: str | None = None,
        schema: Dict[str, Any] | Schema | None = None,
        path: str | None = None,
    ) -> None:
        self.processed_fields = {}
        super().__init__(tap, name, schema, path)

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

        if not results or not results[extract_tag_name(self.records_jsonpath)]:
            return None

        next_page_token = (
            previous_token + self.PAGE_SIZE if previous_token else self.PAGE_SIZE
        )
        return next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if not row.get("customFieldData"):
            return row
        custom_fields = row.pop("customFieldData")
        for field in custom_fields:
            if (
                isinstance(field["value"], dict)
                and field["value"].get("datetime") != None
            ):
                self.processed_fields[(field["field"])["name"]] = field["value"][
                    "datetime"
                ]
            elif (
                isinstance(field["value"], dict) and field["value"].get("value") != None
            ):
                self.processed_fields[(field["field"])["name"]] = field["value"][
                    "value"
                ]
            else:
                self.processed_fields[(field["field"])["name"]] = field["value"]

        for russian, english in self.fields_name_map.items():
            if russian in self.processed_fields:
                self.processed_fields[english] = self.processed_fields.pop(russian)
        row.update(self.processed_fields)
        return row
