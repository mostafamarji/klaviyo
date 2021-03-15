import simplejson
from flask import escape
import json
import os
import requests
import simplejson
import klaviyo

from google.cloud import bigquery

project_id = 'sugatan-290314'
dataset = 'Klaviyo'
table_metrics = 'metrics'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sugatan-290314-c08f69990c42.json"

bigquery_client = bigquery.Client()
dataset_ref = bigquery_client.dataset(dataset)

klaviyo_public_token = "Np5mQ8"
klaviyo_private_token = "pk_e0d671013192332ff5a2cf1519bcb92d70"


def check_string_value(value):
    if value is None:
        return None
    else:
        return value


def check_float_value(value):
    if value is None:
        return None
    else:
        return value


def check_object(dic, item):
    if item in dic:
        return dic[item]
    else:
        return None


def is_float(string):
    try:
        return float(string)
    except ValueError:
        return None


def call_api(requestName):
    table_ref = dataset_ref.table(requestName)
    table = bigquery_client.get_table(table_ref)
    # if requestName == "bounce":
    client = klaviyo.Klaviyo(public_token=klaviyo_public_token, private_token=klaviyo_private_token)
    metrics = client.Metrics.get_metric_timeline_by_id("K8vC8L")
    # metric_json = json.dumps(metrics.data["data"])
    r = json.dumps(metrics.data["data"])
    loaded_r = json.loads(r)
    lst = []

    for i in loaded_r:
        item = {
            'event_properties':
                {
                    'email_domain': check_object(i["event_properties"], "Email Domain"),
                    '_event_id': check_object(i["event_properties"], "$event_id"),
                    '__cohort_message_send_cohort': check_object(
                        i["event_properties"], "$_cohort$message_send_cohort"),
                    '__cohort_variation_send_cohort': check_object(
                        i["event_properties"], "$_cohort$message_send_cohort"),
                    '_variation': check_object(i["event_properties"], "$variation"),
                    # 'attribution': i["event_properties"]["$attribution"],
                    'bounce_type': check_object(i["event_properties"], "Bounce Type"),
                    '_message': check_object(i["event_properties"], "$message"),
                    'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                    '_flow': check_object(i["event_properties"], "$flow"),
                    'subject': check_object(i["event_properties"], "Subject")
                },
            'uuid': i["uuid"],
            'event_name': i["event_name"],
            'timestamp': int(i["timestamp"]),
            'object': i["object"],
            'datetime': i["datetime"],
            'person':
                {
                    'updated': check_object(i["person"], "updated"),
                    '_last_name': check_object(i["person"], "last_name"),
                    '_longitude': is_float(check_object(i["person"], "$longitude")),
                    '_email': check_object(i["person"], "$email"),
                    'object': check_object(i["person"], "object"),
                    '_latitude': is_float(check_object(i["person"], "$latitude")),
                    '_address1': check_object(i["person"], "$address1"),
                    '_address2': check_object(i["person"], "$address2"),
                    '_title': check_object(i["person"], "$title"),
                    '_timezone': check_object(i["person"], "$timezone"),
                    'id': check_object(i["person"], "id"),
                    'first_name': check_object(i["person"], "first_name"),
                    '_organization': check_object(i["person"], "$organization"),
                    '_region': check_object(i["person"], "$region"),
                    'created': check_object(i["person"], "created"),
                    '_phone_number': check_object(i["person"], "$phone_number"),
                    '_country': check_object(i["person"], "$country"),
                    '_source': check_object(i["person"], "$source"),
                    '_zip': check_object(i["person"], "$zip"),
                    '_first_name': check_object(i["person"], "$first_name"),
                    '_city': check_object(i["person"], "$city"),
                    'email': check_object(i["person"], "email"),
                    'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                    'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                    'smile_state': check_object(i["person"], "Smile State"),
                    'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                    '_consent_method': check_object(i["person"], "$consent_method"),
                    '_consent_form_version': check_object(i["person"], "$consent_form_version"),
                    '_consent_form_id':  check_object(i["person"], "$consent_form_id"),
                    '_consent_timestamp':  check_object(i["person"], "$consent_timestamp"),
                    'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                    'smile_points_balance': check_object(i["person"], "Smile Points Balance"),
                    'accepts_marketing': check_object(i["person"], "Accepts Marketing"),

                    # '_consent': {
                    #
                    # }
                    # 'shopify_tags': {
                    #
                    # }
                },
            'statistic_id': i["statistic_id"],
            'id': i["id"]
        }
        lst.append(item)
    # bigquery_client.insert_rows_json(table, lst)
    return lst


if __name__ == '__main__':
    call_api('bounce')
