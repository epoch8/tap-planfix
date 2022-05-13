"""Stream type classes for tap-planfix."""

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_planfix.client import PlanfixStream


class ContactsStream(PlanfixStream):
    name = "planfix_contacts"
    path = "/contact/list"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.IntegerType,
            description=""
        ),
        th.Property(
            "name",
            th.StringType,
            description=""
        ),
        th.Property(
            "lastname",
            th.StringType,
            description=""
        ),
        th.Property(
            "email",
            th.StringType,
            description=""
        ),
        th.Property(
            "phones",
            th.ArrayType(
                th.ObjectType(
                    th.Property("number", th.StringType),
                    th.Property("maskedNumber", th.StringType),
                    th.Property("type", th.IntegerType),
                )

            ),
            description=""
        ),
    ).to_dict()
