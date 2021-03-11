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


def call_api(requestName):
    table_ref = dataset_ref.table(requestName)
    table = bigquery_client.get_table(table_ref)
    # if requestName == "bounce":
    client = klaviyo.Klaviyo(public_token=klaviyo_public_token, private_token=klaviyo_private_token)
    metrics = client.Metrics.get_metric_timeline_by_id("K8vC8L")
    # metric_json = json.dumps(metrics.data["data"])
    lst = []

    for i in metrics.data["data"]:
        item = {
            'event_properties':
                {
                    'email_domain': i["event_properties"]["Email Domain"],
                    '_event_id': i["event_properties"]["$event_id"],
                    '__cohort_message_send_cohort': i["event_properties"]["$_cohort$message_send_cohort"],
                    # 'attribution': i["event_properties"]["$attribution"],
                    'bounce_type': i["event_properties"]["Bounce Type"],
                    '_message': i["event_properties"]["$message"],
                    'campaign_name': i["event_properties"]["Campaign Name"],
                    '_flow': i["event_properties"]["$flow"],
                    'subject': i["event_properties"]["Subject"]
                },
            'uuid': i["uuid"],
            'event_name': i["event_name"],
            'timestamp': i["timestamp"],
            'object': i["object"],
            'datetime': i["datetime"],
            'person':
                {
                    'updated': i["person"]["updated"],
                    '_last_name': i["person"]["last_name"],
                    '_longitude': i["person"]["$longitude"],
                    # '_email': i["person"]["$email"],
                    # 'object': i["person"]["object"],
                    # '_latitude': i["person"]["$latitude"],
                    # '_address1': i["person"]["$address1"],
                    # '_address2': i["person"]["$address2"],
                    # '_title': i["person"]["$title"],
                    # '_timezone': i["person"]["$timezone"],
                    # 'id': i["person"]["id"],
                    # 'first_name': i["person"]["first_name"],
                    # '_organization': i["person"]["$organization"],
                    # '_region': i["person"]["$region"],
                    # 'created': i["person"]["created"],
                    # '_phone_number': i["person"]["$phone_number"],
                    # '_country': i["person"]["$country"],
                    # '_source': i["person"]["$source"],
                    # '_zip': i["person"]["$zip"],
                    # '_first_name': i["person"]["$first_name"],
                    # '_city': i["person"]["$city"],
                    # 'email': i["person"]["email"]
                },
            'statistic_id': i["statistic_id"],
            'id': i["id"]
        }
        lst.append(item)
        bigquery_client.insert_rows_json(table, lst)
        return lst


if __name__ == '__main__':
    call_api('bounce')
