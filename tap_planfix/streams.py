"""Stream type classes for tap-planfix."""
from typing import Optional, Any
from datetime import datetime
import pendulum
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_planfix.client import PlanfixStream


class ContactsStream(PlanfixStream):
    name = "planfix_contacts"
    path = "/contact/list"
    primary_keys = ["id"]  # type: ignore
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
        th.Property("UF_GOOGLE_CID", th.StringType),
        th.Property("UTM markup", th.StringType),
        th.Property("REF", th.StringType),
        th.Property("SiteUserID", th.StringType),
        th.Property("PF id", th.StringType),
        th.Property("User interface language", th.StringType),
        th.Property("Lead (for analytics)", th.BooleanType),
        th.Property("Transition date to Lead+45d", th.StringType),
        th.Property("Date of last message +15d", th.StringType),
        th.Property("Profile", th.StringType),
        th.Property("Contact person", th.StringType),
    ).to_dict()  # type: ignore

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        payload = {
            "offset": next_page_token,
            "pageSize": self.PAGE_SIZE,
            "fields": "id,name,lastname,email,phones,47368,47376,47378,47666,47676,47682,47918,47920,47924,47932,47938",
        }

        return payload

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if not row.get("customFieldData"):
            return row
        custom_fields = row.pop("customFieldData")
        processed_fields = {}
        for field in custom_fields:
            if (
                isinstance(field["value"], dict)
                and field["value"].get("datetime") != None
            ):
                processed_fields[(field["field"])["name"]] = field["value"]["datetime"]
            elif (
                isinstance(field["value"], dict) and field["value"].get("value") != None
            ):
                processed_fields[(field["field"])["name"]] = field["value"]["value"]
            else:
                processed_fields[(field["field"])["name"]] = field["value"]
        if "UTM разметка" in processed_fields:
            processed_fields["UTM markup"] = processed_fields.pop("UTM разметка")
        if "Язык пользовательского интерфейса" in processed_fields:
            processed_fields["User interface language"] = processed_fields.pop(
                "Язык пользовательского интерфейса"
            )
        if "Лид (для аналитики)" in processed_fields:
            processed_fields["Lead (for analytics)"] = processed_fields.pop(
                "Лид (для аналитики)"
            )
        if 'Дата перехода в "Лид"+45д' in processed_fields:
            processed_fields["Transition date to Lead+45d"] = processed_fields.pop(
                'Дата перехода в "Лид"+45д'
            )
        if "Дата посл сообщения +15д" in processed_fields:
            processed_fields["Date of last message +15d"] = processed_fields.pop(
                "Дата посл сообщения +15д"
            )
        row.update(processed_fields)

        return row


class TasksStream(PlanfixStream):
    name = "planfix_tasks"
    path = "/task/list"
    primary_keys = ["id"]  # type: ignore
    replication_key = "updated_at" # type: ignore
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
        starting_timestamp = self.get_starting_timestamp(context) or self.config['start_date']

        payload = {
            "offset": next_page_token,
            "pageSize": self.PAGE_SIZE,
            "filters": [
                {
                    "type": 103,
                    "operator": "gt",
                    "value": {"dateType": "otherDate", "dateValue": f"{starting_timestamp.strftime('%d-%m-%Y')}"},
                    "field": 48148,
                }
            ],
            "fields": "id,name,48148,47184,47234,47390,47680,47556,47602,47208,47210,47212,47224,47228,47238,47240,47242,47246,47248,47252,47268,47270,47294,47296,47300,47582,47864,47656,47658,47660,47662,47876,47306,47344,47364,47520,47522",
        }



        return payload

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description=""),
        th.Property("name", th.StringType, description=""),
        th.Property("updated_at", th.DateTimeType, description=""),
        th.Property("UTM Source", th.StringType),
        th.Property("TourID", th.StringType),
        th.Property("Country", th.StringType),
        th.Property("Promocode", th.StringType),
        th.Property("Prepayment date", th.StringType),
        th.Property("Booking canceling date", th.StringType),
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
        th.Property("UTM_STRING", th.StringType),
        th.Property("UTM Medium", th.StringType),
        th.Property("UTM Campaign", th.StringType),
        th.Property("UTM Content", th.StringType),
        th.Property("UTM Term", th.StringType),
        th.Property("UF_GOOGLE_CID", th.StringType),
        th.Property("Date and time of transition to status", th.StringType),
        th.Property("Lead task", th.StringType),
        th.Property("Cost per lead (priority)", th.StringType),
        th.Property("User FIO", th.StringType),
        th.Property("User email", th.StringType),
    ).to_dict()  # type: ignore

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if not row.get("customFieldData"):
            return row
        custom_fields = row.pop("customFieldData")
        processed_fields = {}
        for field in custom_fields:
            if (
                isinstance(field["value"], dict)
                and field["value"].get("datetime") != None
            ):
                processed_fields[(field["field"])["name"]] = field["value"]["datetime"]
            elif (
                isinstance(field["value"], dict) and field["value"].get("value") != None
            ):
                processed_fields[(field["field"])["name"]] = field["value"]["value"]
            else:
                processed_fields[(field["field"])["name"]] = field["value"]
        if "Страна" in processed_fields:
            processed_fields["Country"] = processed_fields.pop("Страна")
        if "Промокод" in processed_fields:
            processed_fields["Promocode"] = processed_fields.pop("Промокод")
        if "Дата предоплаты" in processed_fields:
            processed_fields["Prepayment date"] = processed_fields.pop(
                "Дата предоплаты"
            )
        if "Дата отмены бронирования" in processed_fields:
            processed_fields["Booking canceling date"] = processed_fields.pop(
                "Дата отмены бронирования"
            )
        if "Дата и время перехода в статус" in processed_fields:
            processed_fields[
                "Date and time of transition to status"
            ] = processed_fields.pop("Дата и время перехода в статус")
        if "Стоимость лида (приоритетность)" in processed_fields:
            processed_fields["Cost per lead (priority)"] = processed_fields.pop(
                "Стоимость лида (приоритетность)"
            )
        if "ФИО пользователя" in processed_fields:
            processed_fields["User FIO"] = processed_fields.pop("ФИО пользователя")
        if "E-mail пользователя" in processed_fields:
            processed_fields["User email"] = processed_fields.pop("E-mail пользователя")
        if "Сконвертирован" in processed_fields:
            processed_fields["Converted"] = processed_fields.pop("Сконвертирован")
        row.update(processed_fields)
        return row
