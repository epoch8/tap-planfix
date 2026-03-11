"""Stream type classes for tap-planfix."""
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from typing import Any, Dict, Optional, Union, List, Iterable, cast, Generator
import datetime

from tap_planfix.client import PlanfixStream
from transliterate import translit


def to_translit(text):
    return translit(
        text
        .replace(" ", "_")
        .replace(",", "_")
        .replace("/", "_")
        .replace("%", "_")
        .replace("&", "_")
        .replace("-", "_")
        .replace(":", "_")
        .replace(")", "")
        .replace("(", "")
        .replace("+", "")
        .replace(".", "")
        .replace('"', "")
        .replace("?", "")
        .replace("'", "")
        .replace(f"\n", "")
        .replace(f"\r", "")
        .replace(f"\t", "")
        .lower(),
        language_code="ru",
        reversed=True,
    )


class ContactsStream(PlanfixStream):
    name = "planfix_contacts"
    path = "/contact/list"
    primary_keys = ["id"]  # type: ignore
    records_jsonpath = "$.contacts[*]"
    filters = []

    fields = "id,name,lastname,email,phones,47368,47376,47378,47666,47676,47682,47918,47920,47924,47932,47938"
    fields_name_map = {
        "UTM разметка": "UTM markup",
        "Язык пользовательского интерфейса": "User interface language",
        "Лид (для аналитики)": "Lead (for analytics)",
        'Дата перехода в "Лид"+45д': "Transition date to Lead+45d",
        "Дата посл сообщения +15д": "Date of last message +15d",
    }

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("email", th.StringType),
        th.Property(
            "phones",
            th.ArrayType(
                th.ObjectType(
                    th.Property("number", th.StringType),
                    th.Property("maskedNumber", th.StringType),
                    th.Property("type", th.IntegerType),
                )
            )
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
        th.Property("offset", th.IntegerType),
        th.Property("upload_timestamp", th.DateTimeType),
    ).to_dict()  # type: ignore


class ContactsLast30DaysStream(PlanfixStream):
    name = "planfix_contacts_last_30_days"
    path = "/contact/list"
    primary_keys = ["id"]  # type: ignore
    records_jsonpath = "$.contacts[*]"
    filters = []
    filter_id = 556256

    fields = "id,name,lastname,email,phones,47368,47376,47378,47666,47676,47682,47918,47920,47924,47932,47938"
    fields_name_map = {
        "UTM разметка": "UTM markup",
        "Язык пользовательского интерфейса": "User interface language",
        "Лид (для аналитики)": "Lead (for analytics)",
        'Дата перехода в "Лид"+45д': "Transition date to Lead+45d",
        "Дата посл сообщения +15д": "Date of last message +15d",
    }

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("email", th.StringType),
        th.Property(
            "phones",
            th.ArrayType(
                th.ObjectType(
                    th.Property("number", th.StringType),
                    th.Property("maskedNumber", th.StringType),
                    th.Property("type", th.IntegerType),
                )
            )
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
        th.Property("offset", th.IntegerType),
        th.Property("upload_timestamp", th.DateTimeType),
    ).to_dict()


class TasksStream(PlanfixStream):
    name = "planfix_tasks"
    path = "/task/list"
    primary_keys = ["id"]  # type: ignore
    # replication_key = "updated_at"  # type: ignore
    records_jsonpath = "$.tasks[*]"
    filters = [
        {
            "type": 51,
            "operator": "equal",
            "value": 446408
        },
        {
            "type": 111,
            "field": 48438,
            "operator": "notequal",
            "value": "2"
        },
    ]
    fields = "id,name,status,48438,48440,47508,48246,47288,48138,48078,47292,48450,assignees,47282,47210,47274,47276,47284,47664,48824,48826,48664,48510,48812"
    # filter_field_type_id = 103
    # filter_field_id = 48148

    fields_name_map = {
        "id": to_translit("id"),
        "name": to_translit("name"),
        "status": to_translit("status"),
        "assignees": to_translit("assignees"),
        48450: to_translit("Тематики обращения"),
        47282: to_translit("Application type"),
        47508: to_translit("D&T of the new request"),
        48438: to_translit("Тэги обращения"),
        48246: to_translit("ДиВ передачи в другой отдел"),
        48440: to_translit("ДиВ создания обращения"),
        47288: to_translit("Date and time of acceptance by the contractor"),
        48138: to_translit("Дата и время первого ответа"),
        47210: to_translit("Prefix"),
        47274: to_translit("Execution result"),
        47292: to_translit("End date and time"),
        48078: to_translit("Дата и время перевода в Обратную связ"),
        47276: to_translit("Grade"),
        47284: to_translit("Customer Priority"),
        47664: to_translit("Test Dev"),
        48824: to_translit("Кол-во комментариев контрагента"),
        48826: to_translit("Кол-во комментариев сотрудника"),
        48664: to_translit("Тип созданного обращения"),
        48510: to_translit("SiteUserID"),
        48812: to_translit("Маршрутизированная группа исполнителей"),
    }

    schema = th.PropertiesList(
        th.Property(to_translit("id"), th.IntegerType),
        th.Property(to_translit("name"), th.StringType),
        th.Property(to_translit("status"), th.StringType),
        th.Property(to_translit("assignees"), th.StringType),
        th.Property(to_translit("Тематики обращения"), th.StringType),
        th.Property(to_translit("Application type"), th.StringType),
        th.Property(to_translit("D&T of the new request"), th.DateTimeType),
        th.Property(to_translit("Тэги обращения"), th.StringType),
        th.Property(to_translit("ДиВ передачи в другой отдел"), th.DateTimeType),
        th.Property(to_translit("ДиВ создания обращения"), th.DateTimeType),
        th.Property(to_translit("Date and time of acceptance by the contractor"), th.DateTimeType),
        th.Property(to_translit("Дата и время первого ответа"), th.DateTimeType),
        th.Property(to_translit("Prefix"), th.StringType),
        th.Property(to_translit("Execution result"), th.StringType),
        th.Property(to_translit("End date and time"), th.DateTimeType),
        th.Property(to_translit("Дата и время перевода в Обратную связ"), th.DateTimeType),
        th.Property(to_translit("Grade"), th.StringType),
        th.Property(to_translit("Customer Priority"), th.StringType),
        th.Property(to_translit("Test Dev"), th.StringType),
        th.Property(to_translit("Кол-во комментариев контрагента"), th.StringType),
        th.Property(to_translit("Кол-во комментариев сотрудника"), th.StringType),
        th.Property(to_translit("Тип созданного обращения"), th.StringType),
        th.Property(to_translit("SiteUserID"), th.StringType),
        th.Property(to_translit("Маршрутизированная группа исполнителей"), th.StringType),
        th.Property("offset", th.IntegerType),
        th.Property("upload_timestamp", th.DateTimeType),
    ).to_dict()  # type: ignore

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:

        dict_task = {}

        dict_task['id'] = row.get('id')
        dict_task['name'] = row.get('name')
        dict_task['status'] = row.get('status', {}).get('name')
        dict_task['assignees'] = ", ".join([f"{assignee.get('name')} ({assignee.get('id')})" for assignee in row.get('assignees', {}).get('users', [])])

        for field in row.get('customFieldData', {}):
            if (key := field.get('field', {}).get('id')) is not None:
                try:
                    if field.get('stringValue') == '':
                        dict_task[key] = None
                    elif key in [47508, 48246, 48440, 47288, 48138, 47292, 48078]:
                        dict_task[key] = datetime.datetime.strptime(field.get('stringValue'), '%d-%m-%Y %H:%M')
                        dict_task[key] = datetime.datetime.strftime(dict_task[key], '%Y-%m-%d %H:%M:%S')
                    elif key in []:
                        dict_task[key] = float(field.get('value'))
                    elif key in []:
                        dict_task[key] = float(str(field.get('value')).replace(' ', ''))
                    elif key in []:
                        dict_task[key] = field.get('value')
                    elif key in [47282, 47210, 47274]:
                        dict_task[key] = field.get('value', {}).get('value')
                    else:
                        dict_task[key] = field.get('stringValue')
                except ValueError as error:
                    self.logger.warning(msg=f"ValueError!\n\n{error}\n\n\tRecord ID:\t{dict_task['id']}\n\tField ID:\t{key}\n\n{field}")
                    dict_task[key] = None

        for name_old, name_new in self.fields_name_map.items():
            if name_old in dict_task:
                dict_task[name_new] = dict_task.pop(name_old)

        dict_task["offset"] = self.offset
        dict_task["upload_timestamp"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S.%f")

        return dict_task


class TasksLast30DaysStream(PlanfixStream):
    name = "planfix_tasks_last_30_days"
    path = "/task/list"
    primary_keys = ["id"]
    records_jsonpath = "$.tasks[*]"
    filters = []
    fields = "id,name,status,48438,48440,47508,48246,47288,48138,48078,47292,48450,assignees,47282,47210,47274,47276,47284,47664,48824,48826,48664,48510,48812"
    filter_id = 556252

    fields_name_map = {
        "id": to_translit("id"),
        "name": to_translit("name"),
        "status": to_translit("status"),
        "assignees": to_translit("assignees"),
        48450: to_translit("Тематики обращения"),
        47282: to_translit("Application type"),
        47508: to_translit("D&T of the new request"),
        48438: to_translit("Тэги обращения"),
        48246: to_translit("ДиВ передачи в другой отдел"),
        48440: to_translit("ДиВ создания обращения"),
        47288: to_translit("Date and time of acceptance by the contractor"),
        48138: to_translit("Дата и время первого ответа"),
        47210: to_translit("Prefix"),
        47274: to_translit("Execution result"),
        47292: to_translit("End date and time"),
        48078: to_translit("Дата и время перевода в Обратную связ"),
        47276: to_translit("Grade"),
        47284: to_translit("Customer Priority"),
        47664: to_translit("Test Dev"),
        48824: to_translit("Кол-во комментариев контрагента"),
        48826: to_translit("Кол-во комментариев сотрудника"),
        48664: to_translit("Тип созданного обращения"),
        48510: to_translit("SiteUserID"),
        48812: to_translit("Маршрутизированная группа исполнителей"),
    }

    schema = th.PropertiesList(
        th.Property(to_translit("id"), th.IntegerType),
        th.Property(to_translit("name"), th.StringType),
        th.Property(to_translit("status"), th.StringType),
        th.Property(to_translit("assignees"), th.StringType),
        th.Property(to_translit("Тематики обращения"), th.StringType),
        th.Property(to_translit("Application type"), th.StringType),
        th.Property(to_translit("D&T of the new request"), th.DateTimeType),
        th.Property(to_translit("Тэги обращения"), th.StringType),
        th.Property(to_translit("ДиВ передачи в другой отдел"), th.DateTimeType),
        th.Property(to_translit("ДиВ создания обращения"), th.DateTimeType),
        th.Property(to_translit("Date and time of acceptance by the contractor"), th.DateTimeType),
        th.Property(to_translit("Дата и время первого ответа"), th.DateTimeType),
        th.Property(to_translit("Prefix"), th.StringType),
        th.Property(to_translit("Execution result"), th.StringType),
        th.Property(to_translit("End date and time"), th.DateTimeType),
        th.Property(to_translit("Дата и время перевода в Обратную связ"), th.DateTimeType),
        th.Property(to_translit("Grade"), th.StringType),
        th.Property(to_translit("Customer Priority"), th.StringType),
        th.Property(to_translit("Test Dev"), th.StringType),
        th.Property(to_translit("Кол-во комментариев контрагента"), th.StringType),
        th.Property(to_translit("Кол-во комментариев сотрудника"), th.StringType),
        th.Property(to_translit("Тип созданного обращения"), th.StringType),
        th.Property(to_translit("SiteUserID"), th.StringType),
        th.Property(to_translit("Маршрутизированная группа исполнителей"), th.StringType),
        th.Property("offset", th.IntegerType),
        th.Property("upload_timestamp", th.DateTimeType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:

        dict_task = {}

        dict_task['id'] = row.get('id')
        dict_task['name'] = row.get('name')
        dict_task['status'] = row.get('status', {}).get('name')
        dict_task['assignees'] = ", ".join([f"{assignee.get('name')} ({assignee.get('id')})" for assignee in row.get('assignees', {}).get('users', [])])

        for field in row.get('customFieldData', {}):
            if (key := field.get('field', {}).get('id')) is not None:
                try:
                    if field.get('stringValue') == '':
                        dict_task[key] = None
                    elif key in [47508, 48246, 48440, 47288, 48138, 47292, 48078]:
                        dict_task[key] = datetime.datetime.strptime(field.get('stringValue'), '%d-%m-%Y %H:%M')
                        dict_task[key] = datetime.datetime.strftime(dict_task[key], '%Y-%m-%d %H:%M:%S')
                    elif key in []:
                        dict_task[key] = float(field.get('value'))
                    elif key in []:
                        dict_task[key] = float(str(field.get('value')).replace(' ', ''))
                    elif key in []:
                        dict_task[key] = field.get('value')
                    elif key in [47282, 47210, 47274]:
                        dict_task[key] = field.get('value', {}).get('value')
                    else:
                        dict_task[key] = field.get('stringValue')
                except ValueError as error:
                    self.logger.warning(msg=f"ValueError!\n\n{error}\n\n\tRecord ID:\t{dict_task['id']}\n\tField ID:\t{key}\n\n{field}")
                    dict_task[key] = None

        for name_old, name_new in self.fields_name_map.items():
            if name_old in dict_task:
                dict_task[name_new] = dict_task.pop(name_old)

        dict_task["offset"] = self.offset
        dict_task["upload_timestamp"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S.%f")

        return dict_task


class Filter555412Stream(PlanfixStream):
    name = "filter__555412"
    path = "/task/list"
    primary_keys = [to_translit("ID Цикла")]
    records_jsonpath = "$.tasks[*]"
    filters = []
    filter_id = "555412"
    fields = "id,name,counterparty,48510,48398,48506,48514,48516,48520,48518,48546,48522,48524,48526,48528,48530,48532,48534,48536,48538,48508,48512,48630"

    fields_name_map = {
        "id": to_translit("ID Цикла"),
        "name": to_translit("Задача"),
        "counterparty.name": to_translit("Контрагент"),
        "counterparty.id": to_translit("ID контакта ПФ"),
        48510: to_translit("SiteUserID"),
        48398: to_translit("Email контакта"),
        48506: to_translit("Дата перехода в Лид"),
        48514: to_translit("Статус лида"),
        48516: to_translit("revenue"),
        48520: to_translit("promocode"),
        48518: to_translit("coupon"),
        48546: to_translit("t_payment for the return tour"),
        48522: to_translit("bonus_amount"),
        48524: to_translit("t_cancellation of the expert's debt (in the currency of the tour)"),
        48526: to_translit("conversionType"),
        48528: to_translit("t_utm_campaign"),
        48530: to_translit("t_utm_content"),
        48532: to_translit("t_utm_medium"),
        48534: to_translit("t_utm_source"),
        48536: to_translit("t_utm_string"),
        48538: to_translit("t_utm_term"),
        48508: to_translit("t_newclient"),
        48512: to_translit("ДиВ изменения статуса лида"),
        48630: to_translit("Travel agency"),
    }

    schema = th.PropertiesList(
        th.Property(to_translit("ID Цикла"), th.IntegerType),
        th.Property(to_translit("Задача"), th.StringType),
        th.Property(to_translit("Контрагент"), th.StringType),
        th.Property(to_translit("ID контакта ПФ"), th.StringType),
        th.Property(to_translit("SiteUserID"), th.StringType),
        th.Property(to_translit("Email контакта"), th.StringType),
        th.Property(to_translit("Дата перехода в Лид"), th.DateTimeType),
        th.Property(to_translit("Статус лида"), th.StringType),
        th.Property(to_translit("revenue"), th.NumberType),
        th.Property(to_translit("promocode"), th.StringType),
        th.Property(to_translit("coupon"), th.NumberType),
        th.Property(to_translit("t_payment for the return tour"), th.NumberType),
        th.Property(to_translit("bonus_amount"), th.NumberType),
        th.Property(to_translit("t_cancellation of the expert's debt (in the currency of the tour)"), th.NumberType),
        th.Property(to_translit("conversionType"), th.StringType),
        th.Property(to_translit("t_utm_campaign"), th.StringType),
        th.Property(to_translit("t_utm_content"), th.StringType),
        th.Property(to_translit("t_utm_medium"), th.StringType),
        th.Property(to_translit("t_utm_source"), th.StringType),
        th.Property(to_translit("t_utm_string"), th.StringType),
        th.Property(to_translit("t_utm_term"), th.StringType),
        th.Property(to_translit("t_newclient"), th.StringType),
        th.Property(to_translit("ДиВ изменения статуса лида"), th.DateTimeType),
        th.Property(to_translit("Travel agency"), th.BooleanType),
        th.Property("offset", th.IntegerType),
        th.Property("upload_timestamp", th.DateTimeType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:

        dict_task = {}

        dict_task['id'] = row.get('id')
        dict_task['name'] = row.get('name')
        dict_task['counterparty.id'] = row.get('counterparty', {}).get('id')
        dict_task['counterparty.name'] = row.get('counterparty', {}).get('name')

        for field in row.get('customFieldData', {}):
            if (key := field.get('field', {}).get('id')) is not None:
                try:
                    if field.get('stringValue') == '':
                        dict_task[key] = None
                    elif key in [48506, 48512]:
                        dict_task[key] = datetime.datetime.strptime(field.get('stringValue'), '%d-%m-%Y %H:%M')
                        dict_task[key] = datetime.datetime.strftime(dict_task[key], '%Y-%m-%d %H:%M:%S')
                    elif key in [48516, 48546, 48522, 48524]:
                        dict_task[key] = float(field.get('value'))
                    elif key in [48518]:
                        dict_task[key] = float(str(field.get('value')).replace(' ', ''))
                    elif key in [48630]:
                        dict_task[key] = field.get('value')
                    else:
                        dict_task[key] = field.get('stringValue')
                except ValueError as error:
                    self.logger.warning(msg=f"ValueError!\n\n{error}\n\n\tRecord ID:\t{dict_task['id']}\n\tField ID:\t{key}\n\n{field}")
                    dict_task[key] = None

        for name_old, name_new in self.fields_name_map.items():
            if name_old in dict_task:
                dict_task[name_new] = dict_task.pop(name_old)

        dict_task["offset"] = self.offset
        dict_task["upload_timestamp"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S.%f")

        return dict_task


class Filters511174931Stream(PlanfixStream):
    name = "filters_51_1174931"
    path = "/task/list"
    primary_keys = [to_translit("id")]
    records_jsonpath = "$.tasks[*]"
    filters = [
        {
            "type": 51,
            "operator": "equal",
            "value": 1174931,
        }
    ]
    fields = "id,name,47556,48040,47486,48104,47248,48080,48288,counterparty,48398,48514,48510,48296,48282,47254,status"

    fields_name_map = {
        "id": to_translit("id"),
        "name": to_translit("name"),
        "status": to_translit("status"),
        "counterparty": to_translit("counterparty"),
        47248: to_translit("Revenue in rubles"),
        47254: to_translit("Write-off of expert's debt (in tour currency)"),
        47486: to_translit("Сотрудник"),
        47556: to_translit("Дата предоплаты"),
        48040: to_translit("Номер предоплаты"),
        48080: to_translit("Доплата за возвратный тур (+10%), рубли"),
        48104: to_translit("Вклад в сделку"),
        48282: to_translit("Сумма бонусов, руб"),
        48288: to_translit("Комиссия по допуслуге, руб"),
        48296: to_translit("Сумма промокодов, руб (число)"),
        48398: to_translit("E-mail контрагента"),
        48510: to_translit("SiteUserID"),
        48514: to_translit("state"),
    }

    schema = th.PropertiesList(
        th.Property(to_translit("id"), th.IntegerType),
        th.Property(to_translit("name"), th.StringType),
        th.Property(to_translit("status"), th.StringType),
        th.Property(to_translit("counterparty"), th.StringType),
        th.Property(to_translit("Revenue in rubles"), th.NumberType),
        th.Property(to_translit("Write-off of expert's debt (in tour currency)"), th.NumberType),
        th.Property(to_translit("Сотрудник"), th.StringType),
        th.Property(to_translit("Дата предоплаты"), th.DateTimeType),
        th.Property(to_translit("Номер предоплаты"), th.StringType),
        th.Property(to_translit("Доплата за возвратный тур (+10%), рубли"), th.NumberType),
        th.Property(to_translit("Вклад в сделку"), th.StringType),
        th.Property(to_translit("Сумма бонусов, руб"), th.NumberType),
        th.Property(to_translit("Комиссия по допуслуге, руб"), th.NumberType),
        th.Property(to_translit("Сумма промокодов, руб (число)"), th.NumberType),
        th.Property(to_translit("E-mail контрагента"), th.StringType),
        th.Property(to_translit("SiteUserID"), th.StringType),
        th.Property(to_translit("state"), th.StringType),
        th.Property("offset", th.IntegerType),
        th.Property("upload_timestamp", th.DateTimeType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:

        dict_task = {}

        dict_task["id"] = row.get("id")
        dict_task["name"] = row.get("name")
        dict_task["status"] = row.get("status", {}).get("name")
        dict_task["counterparty"] = row.get("counterparty", {}).get("name")

        for field in row.get("customFieldData", {}):
            if (key := field.get("field", {}).get("id")) is not None:
                try:
                    if field.get("stringValue") == "":
                        dict_task[key] = None
                    elif key in [47556]:
                        dict_task[key] = datetime.datetime.strptime(field.get("stringValue"), "%d-%m-%Y %H:%M")
                        dict_task[key] = datetime.datetime.strftime(dict_task[key], "%Y-%m-%d %H:%M:%S")
                    elif key in [47248, 47254, 48080, 48282, 48288, 48296]:
                        dict_task[key] = float(field.get("value"))
                    elif key in []:
                        dict_task[key] = float(str(field.get("value")).replace(" ", ""))
                    elif key in []:
                        dict_task[key] = field.get("value")
                    elif key in []:
                        dict_task[key] = field.get("value", {}).get("value")
                    else:
                        dict_task[key] = field.get("stringValue")
                except ValueError as error:
                    self.logger.warning(msg=f"ValueError!\n\n{error}\n\n\tRecord ID:\t{dict_task['id']}\n\tField ID:\t{key}\n\n{field}")
                    dict_task[key] = None

        for name_old, name_new in self.fields_name_map.items():
            if name_old in dict_task:
                dict_task[name_new] = dict_task.pop(name_old)

        dict_task["offset"] = self.offset
        dict_task["upload_timestamp"] = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S.%f")

        return dict_task
