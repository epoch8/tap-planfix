"""Stream type classes for tap-planfix."""
from typing import Optional, Any, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_planfix.client import PlanfixStream


class ContactsStream(PlanfixStream):
    name = "planfix_contacts"
    path = "/contact/list"
    primary_keys = ["id"]
    records_jsonpath = "$.contacts[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description=""),
        th.Property("name", th.StringType, description=""),
        th.Property("lastname", th.StringType, description=""),
        th.Property("email", th.StringType, description=""),
        th.Property(
            "phones",
            th.ArrayType(
                th.ObjectType(
                    th.Property("number", th.StringType),
                    th.Property("maskedNumber", th.StringType),
                    th.Property("type", th.IntegerType),
                )
            ),
            description="",
        ),
    ).to_dict()


class TasksStream(PlanfixStream):
    name = "planfix_tasks"
    path = "/task/list"
    primary_keys = ["id"]
    records_jsonpath = "$.tasks[*]"

    def get_next_page_token(self, response, previous_token):
        """Return a token for identifying next page or None if no more pages."""
        results = response.json()

        if not results or not results["tasks"]:
            return None

        next_page_token = (
            previous_token + self.PAGE_SIZE if previous_token else self.PAGE_SIZE
        )
        return next_page_token

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        payload = {
            "offset": next_page_token,
            "pageSize": self.PAGE_SIZE,
            "fields": "id,name,47184,47234,47390,47680,47556,47602,47208,47210,47212,47224,47228,47238,47240,47242,47246,47248,47252,47268,47270,47294,47296,47300,47582",
        }

        return payload

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description=""),
        th.Property("name", th.StringType, description=""),
        th.Property("UTM Source", th.StringType),
        th.Property("TourID", th.StringType),
        th.Property("Country", th.StringType),
        th.Property("Promocode", th.StringType),
        th.Property("Prepayment date", th.StringType),
        th.Property("Дата отмены бронирования", th.StringType),
        th.Property("Budget", th.StringType),
        th.Property("Prefix", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("Source", th.StringType),
        th.Property("Converted", th.StringType),
        th.Property("Booking", th.StringType),
        th.Property("Payment", th.StringType),
        th.Property("Language", th.StringType),
        th.Property("Gross Profit", th.StringType),
        th.Property("Revenue in rubles", th.StringType),
        th.Property("Paid with bonuses", th.StringType),
        th.Property("Payment Gateway", th.StringType),
        th.Property("Paid", th.StringType),
        th.Property("Prepayment amount", th.StringType),
        th.Property("Remaining amount", th.StringType),
        th.Property("Postpaid number", th.StringType),
        th.Property("Order number", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        custom_fields = row.pop("customFieldData")
        processed_fields = {}
        if custom_fields:
            for field in custom_fields:
                if isinstance(field["value"], dict):
                    processed_fields[(field["field"])["name"]] = field["value"]["value"]
                else:
                    processed_fields[(field["field"])["name"]] = field["value"]
        processed_fields["Country"] = processed_fields.pop("Страна")
        processed_fields["Promocode"] = processed_fields.pop("Промокод")
        processed_fields["Prepayment date"] = processed_fields.pop("Дата предоплаты")
        processed_fields["Booking canceling date"] = processed_fields.pop("Дата отмены бронирования")
        row.update(processed_fields)
        return row

