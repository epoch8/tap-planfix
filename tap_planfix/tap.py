"""Planfix tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_planfix.streams import PlanfixStream, ContactsStream, TasksStream

STREAM_TYPES = [
    ContactsStream,
    TasksStream
]


class TapPlanfix(Tap):
    """Planfix tap class."""

    name = "tap-planfix"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "planfiix_url",
            th.StringType,
        ),
        th.Property("planfix_token", th.StringType),
        th.Property("start_date", th.DateType),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
