"""Planfix tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_planfix.streams import (
    ContactsStream,
    TasksStream,
    Filter555412Stream,
    Filters511174931Stream,
)

STREAM_TYPES = [
    ContactsStream,
    TasksStream,
    Filter555412Stream,
    Filters511174931Stream,
]


class TapPlanfix(Tap):
    """Planfix tap class."""

    name = "tap-planfix"

    config_jsonschema = th.PropertiesList(
        th.Property("planfix_url", th.StringType, required=True),
        th.Property("planfix_token", th.StringType, required=True),
        th.Property("start_date", th.DateType, required=False),
        th.Property("page_size", th.IntegerType, required=False, default=100),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
