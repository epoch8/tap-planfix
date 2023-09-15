"""Stream type classes for tap-planfix."""
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_planfix.client import PlanfixStream


class ContactsStream(PlanfixStream):
    name = "planfix_contacts"
    path = "/contact/list"
    primary_keys = ["id"]  # type: ignore
    records_jsonpath = "$.contacts[*]"
    fields = "id,name,lastname,email,phones,47368,47376,47378,47666,47676,47682,47918,47920,47924,47932,47938"
    fields_name_map = {
        "UTM разметка": "UTM markup",
        "Язык пользовательского интерфейса": "User interface language",
        "Лид (для аналитики)": "Lead (for analytics)",
        'Дата перехода в "Лид"+45д': "Transition date to Lead+45d",
        "Дата посл сообщения +15д": "Date of last message +15d",
    }
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


class TasksStream(PlanfixStream):
    name = "planfix_tasks"
    path = "/task/list"
    primary_keys = ["id"]  # type: ignore
    replication_key = "updated_at"  # type: ignore
    records_jsonpath = "$.tasks[*]"
    fields = "id,name,48148,47184,47234,47390,47680,47556,47602,47208,47210,47212,47224,47228,47238,47240,47242,47246,47248,47252,47268,47270,47294,47296,47300,47582,47864,47656,47658,47660,47662,47876,47306,47344,47364,47520,47522"
    filter_field_type_id = 103
    filter_field_id = 48148
    fields_name_map = {
        "Страна": "Country",
        "Промокод": "Promocode",
        "Дата предоплаты": "Prepayment date",
        "Дата отмены бронирования": "Booking canceling date",
        "Дата и время перехода в статус": "Date and time of transition to status",
        "Стоимость лида (приоритетность)": "Cost per lead (priority)",
        "ФИО пользователя": "User FIO",
        "E-mail пользователя": "User email",
        "Сконвертирован": "Converted",
    }

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


class CashInflowStream(PlanfixStream):
    name = "planfix_cash_in"
    path = "/datatag/7052/entry/list"
    primary_keys = ["key"]  # type: ignore
    replication_key = "created_at"  # type: ignore
    records_jsonpath = "$.dataTagEntries[*]"
    fields = "dataTag,key,30008,30010,30012,30014,30016,30018"
    filter_field_type_id = 3101
    filter_field_id = 30008
    fields_name_map = {
        "Сумма": "Sum",
        "Валюта": "Currency",
        "Курс": "Exchange_rate",
        "Дата и время": "created_at",
        "Сумма, РУБ": "Sum_rub",
        "Шлюз": "Gateway",
    }
    schema = th.PropertiesList(
        th.Property("dataTag", th.StringType, description=""),
        th.Property("key", th.IntegerType, description=""),
        th.Property("created_at", th.DateTimeType, description=""),
        th.Property("Sum", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("Exchange_rate", th.StringType),
        th.Property("Sum_rub", th.StringType),
        th.Property("Gateway", th.StringType),
    ).to_dict()  # type: ignore


class CompletedRequestsStream(PlanfixStream):
    name = "planfix_completed_request"
    path = "/datatag/7064/entry/list"
    primary_keys = ["key"]  # type: ignore
    replication_key = "Finished_at_datetime"  # type: ignore
    records_jsonpath = "$.dataTagEntries[*]"
    fields = "dataTag,key,30094,30108,30096,30098,30348,30100,30262,30102,30104,30106,30110,30120"
    filter_field_type_id = 3101
    filter_field_id = 30098
    fields_name_map = {
        "Исполнитель": "Executor",
        "Дата и время нового обращения": "New_request_datetime",
        "Дата и время первого ответа": "First_response_datetime",
        "Дата и время завершения": "Finished_at_datetime",
        "Дата и время перехода в Обратную связь": "Transition_to_feedback_datetime",
        "Оценка": "Score",
        "Тематика обращения": "Subject",
        "Приоритет": "Priority",
        "Результат выполнения": "Result",
        "Дата и время создания аналитики": "Analytics_created_at_datetime",
        "Префикс": "Prefix",
    }
    schema = th.PropertiesList(
        th.Property("dataTag", th.StringType),
        th.Property("key", th.IntegerType),
        th.Property("Executor", th.StringType),
        th.Property("New_request_datetime", th.DateTimeType),
        th.Property("First_response_datetime", th.DateTimeType),
        th.Property("Finished_at_datetime", th.DateTimeType),
        th.Property("Transition_to_feedback_datetime", th.DateTimeType),
        th.Property("Score", th.StringType),
        th.Property("Subject", th.StringType),
        th.Property("Priority", th.StringType),
        th.Property("Result", th.StringType),
        th.Property("Analytics_created_at_datetime", th.DateTimeType),
        th.Property("Prefix", th.StringType),
    ).to_dict()  # type: ignore


class FirstResponseStream(PlanfixStream):
    name = "planfix_first_response"
    path = "/datatag/7066/entry/list"
    primary_keys = ["key"]  # type: ignore
    replication_key = "First_response_datetime"  # type: ignore
    records_jsonpath = "$.dataTagEntries[*]"
    fields = "dataTag,key,30112,30114,30116,30118,30220"
    filter_field_type_id = 3101
    filter_field_id = 30116
    fields_name_map = {
        "Исполнитель": "Executor",
        "Дата обращения": "Request_datetime",
        "Дата первого ответа": "First_response_datetime",
        "Тип обращения": "Request_type",
        "Префикс": "Prefix",
    }
    schema = th.PropertiesList(
        th.Property("dataTag", th.StringType),
        th.Property("key", th.IntegerType),
        th.Property("Executor", th.StringType),
        th.Property("Request_datetime", th.DateTimeType),
        th.Property("First_response_datetime", th.DateTimeType),
        th.Property("Request_type", th.StringType),
        th.Property("Prefix", th.StringType),
    ).to_dict()  # type: ignore


class TaskAcceptanceStream(PlanfixStream):
    name = "planfix_task_acceptance"
    path = "/datatag/7092/entry/list"
    primary_keys = ["key"]  # type: ignore
    replication_key = "Acceptance_datetime"  # type: ignore
    records_jsonpath = "$.dataTagEntries[*]"
    fields = "dataTag,key,30308,30310"
    filter_field_type_id = 3101
    filter_field_id = 30308
    fields_name_map = {
        "Принявший сотрудник": "Accepted_employee",
        "Дата и Время принятия": "Acceptance_datetime",
    }
    schema = th.PropertiesList(
        th.Property("dataTag", th.StringType),
        th.Property("key", th.IntegerType),
        th.Property("Acceptance_datetime", th.DateTimeType),
        th.Property("Accepted_employee", th.StringType),
    ).to_dict()  # type: ignore


class LeadsStream(PlanfixStream):
    name = "planfix_leads"
    path = "/datatag/7098/entry/list"
    primary_keys = ["key"]  # type: ignore
    # replication_key = "Lead_creation_datetime"  # type: ignore
    records_jsonpath = "$.dataTagEntries[*]"
    fields = "dataTag,key,30350,30352,30382,30384,30386,30388,30398,30404,30362,30364,30366"
    filter_field_type_id = 3101
    filter_field_id = 30388
    fields_name_map = {
        "Исполнитель": "Executor",
        "Лид": "Lead",
        "Страна лида": "Lead_country",
        "Почта лида": "Lead_mail",
        "Телефон лида": "Lead_phone",
        "Дата создания лида": "Lead_creation_datetime",
        "Дата и время последнего обращения": "Last_request_datetime",
        "Д и В последнего обращения": "Last_request_dt",
        "Источник": "Source",
        "Префикс": "Prefix",
        "Язык пользовательского интерфейса": "UI_language",
    }
    schema = th.PropertiesList(
        th.Property("dataTag", th.StringType),
        th.Property("key", th.IntegerType),
        th.Property("Executor", th.StringType),
        th.Property("Lead", th.StringType),
        th.Property("Lead_country", th.StringType),
        th.Property("Lead_mail", th.StringType),
        th.Property("Lead_phone", th.StringType),
        th.Property("Source", th.StringType),
        th.Property("Prefix", th.StringType),
        th.Property("UI_language", th.StringType),
        th.Property("Lead_creation_datetime", th.StringType),
        th.Property("Last_request_datetime", th.DateTimeType),
        th.Property("Last_request_dt", th.DateTimeType),
    ).to_dict()  # type: ignore


class ContributionToDealStream(PlanfixStream):
    name = "planfix_contribution_to_deal"
    path = "/datatag/7100/entry/list"
    primary_keys = ["key"]  # type: ignore
    replication_key = "Push_datetime"  # type: ignore
    records_jsonpath = "$.dataTagEntries[*]"
    fields = "dataTag,key,30374,30370,30394"
    filter_field_type_id = 3101
    filter_field_id = 30374
    fields_name_map = {
        "Дата пуша": "Push_datetime",
        "Исполнитель": "Executor",
        "Вклад в сделку": "Contribution_to_deal",
    }
    schema = th.PropertiesList(
        th.Property("dataTag", th.StringType),
        th.Property("key", th.IntegerType),
        th.Property("Executor", th.StringType),
        th.Property("Contribution_to_deal", th.StringType),
        th.Property("Push_datetime", th.DateTimeType),

    ).to_dict()  # type: ignore


class PingsStream(PlanfixStream):
    name = "planfix_pings"
    path = "/datatag/7086/entry/list"
    primary_keys = ["key"]  # type: ignore
    records_jsonpath = "$.dataTagEntries[*]"
    fields = "dataTag,key,30312,30326,30328,30330,30332,30334,30336,30324"
    fields_name_map = {
        "Путешественник": "Traveler",
        "Дата создания Лида": "Lead_creation_datetime",
        "Дата 15мин пинг": "15_min_ping_datetime",
        "Дата 1 пинга": "1_ping_datetime",
        "Дата 2 пинга": "2_ping_datetime",
        "Дата 3 пинга": "3_ping_datetime",
        "Дата конверсии": "Conversion_datetime",
        "Дата снятия метки \"Лид\"": "Lead_removed_datetime",
    }
    schema = th.PropertiesList(
        th.Property("dataTag", th.StringType),
        th.Property("key", th.IntegerType),
        th.Property("Traveler", th.StringType),
        th.Property("Lead_creation_datetime", th.DateTimeType),
        th.Property("15_min_ping_datetime", th.DateTimeType),
        th.Property("1_ping_datetime", th.DateTimeType),
        th.Property("2_ping_datetime", th.DateTimeType),
        th.Property("3_ping_datetime", th.DateTimeType),
        th.Property("Conversion_datetime", th.DateTimeType),
        th.Property("Lead_removed_datetime", th.DateTimeType),

    ).to_dict()  # type: ignore
