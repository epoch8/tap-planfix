"""Planfix tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_planfix.streams import (
    PlanfixStream,
    ContactsStream
)

STREAM_TYPES = [
    ContactsStream
]


class TapPlanfix(Tap):
    """Planfix tap class."""
    name = "tap-planfix"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "planfiix_url",
            th.StringType,
        ),
        th.Property(
            "planfix_token",
            th.StringType
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

