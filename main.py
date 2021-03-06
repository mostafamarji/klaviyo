from datetime import datetime, timedelta

import json
import os
import time
from collections import OrderedDict

import klaviyo
from google.cloud import bigquery
import asyncio

project_id = 'sugatan-290314'
dataset = 'Klaviyo'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sugatan-290314-c08f69990c42.json"

bigquery_client = bigquery.Client()
dataset_ref = bigquery_client.dataset(dataset)

# sugatan client
# klaviyo_public_token = "Np5mQ8"
# klaviyo_private_token = "pk_180837f7a9ab7e0dc3672c9e56a865c53d"

# new client
klaviyo_public_token = "Lms8DK"
klaviyo_private_token = "pk_047cdab02ed5ee4e56970938d3f8ee8187"
client = klaviyo.Klaviyo(public_token=klaviyo_public_token, private_token=klaviyo_private_token)

final_lst = []
next_data = ""


def check_object(dic, item):
    if item in dic:
        return dic[item]
    else:
        return ""


def is_float(param):
    try:
        if param == "":
            return 0
        else:
            return float(param)
    except ValueError:
        return 0


def is_integer(param):
    try:
        if param == "":
            return 0
        else:
            return int(param)
    except ValueError:
        return 0


def is_bool(param):
    try:
        if param == "":
            return "False"
        else:
            return str(param)
    except ValueError:
        return "False"


def get_metric_data(metrickey, sincedata, fromdate, todate, tablename, table):
    global final_lst, next_data

    try:
        allow_get_next_data = True
        metrics = client.Metrics.get_metric_timeline_by_id(metrickey, since=sincedata, sort="desc")
        # await asyncio.sleep(1)
        r = json.dumps(metrics.data["data"])
        loaded_r = json.loads(r)
        if len(loaded_r) == 0:
            print("No data found " + tablename)
        # print(loaded_r)
        # get_table_properties(loaded_r)
        for i in loaded_r:
            record_date = datetime.fromisoformat(i["datetime"]).date()
            from_date = datetime.fromisoformat(fromdate.replace("/", "-")).date()
            to_date = datetime.fromisoformat(todate.replace("/", "-")).date()
            if from_date <= record_date <= to_date:
                if tablename == "dropped_email":
                    record = create_dropped_email_item(i)
                    final_lst.append(record)

                if tablename == "click":
                    record = create_click_data_item(i)
                    final_lst.append(record)

                if tablename == "open":
                    record = create_open_data_item(i)
                    final_lst.append(record)

                if tablename == "bounce":
                    record = create_bounce_data_item(i)
                    final_lst.append(record)

                if tablename == "mark_as_spam":
                    record = create_mark_as_spam_data_item(i)
                    final_lst.append(record)

                if tablename == "receive":
                    record = create_receive_data_item(i)
                    final_lst.append(record)

                if tablename == "subscribe_list":
                    record = create_subscribe_list_data_item(i)
                    final_lst.append(record)

                if tablename == "unsub_list":
                    record = create_unsub_list_data_item(i)
                    final_lst.append(record)

                if tablename == "unsubscribe":
                    record = create_unsubscribe_data_item(i)
                    final_lst.append(record)
                if tablename == "update_email_preferences":
                    record = create_update_email_preferences_data_item(i)
                    final_lst.append(record)

            else:
                allow_get_next_data = False
                break
        if len(final_lst) > 0:
            result = bigquery_client.insert_rows_json(table, final_lst)
            print("Data is saved to big query")
        if (metrics.data["next"] != "") & (str(metrics.data["next"]).lower() != "none") & allow_get_next_data:
            final_lst = []
            # time.sleep(3)
            print("Getting next data for " + tablename)
            next_data = metrics.data["next"]
            get_metric_data(metrickey, metrics.data["next"], fromdate, todate, tablename, table)
        else:
            return "done"
    except Exception as e:
        final_lst = []
        print("Waiting after exception")
        time.sleep(30)
        if (next_data != "") & (next_data.lower() != "none"):
            get_metric_data(metrickey, next_data, fromdate, todate, tablename, table)
        else:
            get_metric_data(metrickey, fromdate, fromdate, todate, tablename, table)


def update_big_query_schema():
    table_ref = dataset_ref.table("bounce")
    table = bigquery_client.get_table(table_ref)
    original_schema = table.schema  # Get your current table's schema
    new_schema = original_schema[:]  # Creates a copy of the schema.
    # Add new field to schema
    new_schema.append(bigquery.SchemaField("new_field", "STRING"))
    # Set new schema in your table object
    table.schema = new_schema
    # Call API to update your table with the new schema
    table = client.update_table(table, ["schema"])


def get_table_properties(loaded_r):
    person_list = []
    for o in loaded_r:
        for att in o['event_properties']:
            person_list.append(att)
    result = list(OrderedDict.fromkeys(person_list))
    for j in result:
        final_lst.append(j)
    result = list(OrderedDict.fromkeys(final_lst))
    print(result)


def create_dropped_email_item(i):
    item = {
        'event_properties':
            {
                'email_domain': check_object(i["event_properties"], "Email Domain"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                '_cohort_message_send_cohort': check_object(i["event_properties"],
                                                            "$_cohort$message_send_cohort"),
                '_variation': check_object(i["event_properties"], "$variation"),
                '_cohort_variation_send_cohort': check_object(i["event_properties"],
                                                              "$_cohort$variation_send_cohort"),
                '_message': check_object(i["event_properties"], "$message"),
                'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                'subject': check_object(i["event_properties"], "Subject"),

                # 'attribution': i["event_properties"]["$attribution"],
                # 'extra': i["event_properties"]["$attribution"],

            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_click_data_item(i):
    item = {
        'event_properties':
            {
                'email_domain': check_object(i["event_properties"], "Email Domain"),

                'client_name': check_object(i["event_properties"], "Client Name"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                '_cohort_message_send_cohort': check_object(i["event_properties"],
                                                            "$_cohort$message_send_cohort"),
                'url': check_object(i["event_properties"], "URL"),
                '_variation': check_object(i["event_properties"], "$variation"),
                '_cohort_variation_send_cohort': check_object(i["event_properties"],
                                                              "$_cohort$variation_send_cohort"),
                'client_os': check_object(i["event_properties"], "Client OS"),
                '_message_interaction': check_object(i["event_properties"], "$message_interaction"),
                '_message': check_object(i["event_properties"], "$message"),
                'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                '_flow': check_object(i["event_properties"], "$flow"),
                'client_os_family': check_object(i["event_properties"], "Client OS Family"),
                'client_type': check_object(i["event_properties"], "Client Type"),
                'subject': check_object(i["event_properties"], "Subject"),

                # 'attribution': i["event_properties"]["$attribution"],
                # 'extra': i["event_properties"]["$attribution"],

            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_open_data_item(i):
    item = {
        'event_properties':
            {
                'email_domain': check_object(i["event_properties"], "Email Domain"),

                'client_name': check_object(i["event_properties"], "Client Name"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                '_cohort_message_send_cohort': check_object(i["event_properties"],
                                                            "$_cohort$message_send_cohort"),
                'client_canonical': check_object(i["event_properties"], "Client Canonical"),
                '_variation': check_object(i["event_properties"], "$variation"),
                '_cohort_variation_send_cohort': check_object(i["event_properties"],
                                                              "$_cohort$variation_send_cohort"),
                'client_os': check_object(i["event_properties"], "Client OS"),
                '_message_interaction': check_object(i["event_properties"], "$message_interaction"),
                '_message': check_object(i["event_properties"], "$message"),
                'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                '_flow': check_object(i["event_properties"], "$flow"),
                'client_os_family': check_object(i["event_properties"], "Client OS Family"),
                'client_type': check_object(i["event_properties"], "Client Type"),
                'subject': check_object(i["event_properties"], "Subject"),

                # 'attribution': i["event_properties"]["$attribution"],
                # 'extra': i["event_properties"]["$attribution"],

            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                # 'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                # "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_bounce_data_item(i):
    item = {
        'event_properties':
            {
                '_event_id': check_object(i["event_properties"], "$event_id"),
                'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                '_flow': check_object(i["event_properties"], "$flow"),
                '_variation': check_object(i["event_properties"], "$variation"),
                'subject': check_object(i["event_properties"], "Subject"),
                '_cohort_variation_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),
                '_cohort_message_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),
                '_message': check_object(i["event_properties"], "$message"),
                'email_domain': check_object(i["event_properties"], "Email Domain"),
                'bounce_type': check_object(i["event_properties"], "Bounce Type"),

                # 'attribution': i["event_properties"]["$attribution"],
                # 'extra': i["event_properties"]["$attribution"],

            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_mark_as_spam_data_item(i):
    item = {
        'event_properties':
            {
                'email_domain': check_object(i["event_properties"], "Email Domain"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                '_cohort_message_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),
                '_message': check_object(i["event_properties"], "$message"),
                'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                '_flow': check_object(i["event_properties"], "$flow"),
                'subject': check_object(i["event_properties"], "Subject"),
                '_variation': check_object(i["event_properties"], "$variation"),
                '_cohort_variation_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),

                # 'attribution': i["event_properties"]["$attribution"],
                # 'extra': i["event_properties"]["$attribution"],
            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_receive_data_item(i):
    item = {
        'event_properties':
            {
                'email_domain': check_object(i["event_properties"], "Email Domain"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                '_cohort_message_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),
                '_message': check_object(i["event_properties"], "$message"),
                'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                '_flow': check_object(i["event_properties"], "$flow"),
                'subject': check_object(i["event_properties"], "Subject"),
                '_variation': check_object(i["event_properties"], "$variation"),
                '_cohort_variation_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),

                # 'attribution': i["event_properties"]["$attribution"],
                # 'extra': i["event_properties"]["$attribution"],
            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_subscribe_list_data_item(i):
    item = {
        'event_properties':
            {
                'list': check_object(i["event_properties"], "List"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                # '_cohort_message_send_cohort': check_object(
                #     i["event_properties"], "$_cohort$message_send_cohort"),
                # '_message': check_object(i["event_properties"], "$message"),
                # 'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                # '_flow': check_object(i["event_properties"], "$flow"),
                # 'subject': check_object(i["event_properties"], "Subject"),
                # '_variation': check_object(i["event_properties"], "$variation"),
                # '_cohort_variation_send_cohort': check_object(
                #     i["event_properties"], "$_cohort$message_send_cohort"),
                #
                # # 'attribution': i["event_properties"]["$attribution"],
                # # 'extra': i["event_properties"]["$attribution"],
            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_unsub_list_data_item(i):
    item = {
        'event_properties':
            {
                'list': check_object(i["event_properties"], "List"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                # '_cohort_message_send_cohort': check_object(
                #     i["event_properties"], "$_cohort$message_send_cohort"),
                # '_message': check_object(i["event_properties"], "$message"),
                # 'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                # '_flow': check_object(i["event_properties"], "$flow"),
                # 'subject': check_object(i["event_properties"], "Subject"),
                # '_variation': check_object(i["event_properties"], "$variation"),
                # '_cohort_variation_send_cohort': check_object(
                #     i["event_properties"], "$_cohort$message_send_cohort"),
                #
                # # 'attribution': i["event_properties"]["$attribution"],
                # # 'extra': i["event_properties"]["$attribution"],
            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_unsubscribe_data_item(i):
    item = {
        'event_properties':
            {
                'email_domain': check_object(i["event_properties"], "Email Domain"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                '_cohort_message_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),
                '_variation': check_object(i["event_properties"], "$variation"),
                '_cohort_variation_send_cohort': check_object(
                    i["event_properties"], "$_cohort$message_send_cohort"),
                '_message': check_object(i["event_properties"], "$message"),
                'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                '_flow': check_object(i["event_properties"], "$flow"),
                'subject': check_object(i["event_properties"], "Subject"),

                # 'attribution': i["event_properties"]["$attribution"],
                # 'extra': i["event_properties"]["$attribution"],
            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_update_email_preferences_data_item(i):
    item = {
        'event_properties':
            {
                'list': check_object(i["event_properties"], "List"),
                '_event_id': check_object(i["event_properties"], "$event_id"),
                # '_cohort_message_send_cohort': check_object(
                #     i["event_properties"], "$_cohort$message_send_cohort"),
                # '_message': check_object(i["event_properties"], "$message"),
                # 'campaign_name': check_object(i["event_properties"], "Campaign Name"),
                # '_flow': check_object(i["event_properties"], "$flow"),
                # 'subject': check_object(i["event_properties"], "Subject"),
                # '_variation': check_object(i["event_properties"], "$variation"),
                # '_cohort_variation_send_cohort': check_object(
                #     i["event_properties"], "$_cohort$message_send_cohort"),
                #
                # # 'attribution': i["event_properties"]["$attribution"],
                # # 'extra': i["event_properties"]["$attribution"],
            },
        'person':
            {
                'updated': check_object(i["person"], "updated"),
                'last_name': check_object(i["person"], "last_name"),
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
                '_id': check_object(i["person"], "$id"),
                'created': check_object(i["person"], "created"),
                '_last_name': check_object(i["person"], "$last_name"),
                '_phone_number': check_object(i["person"], "$phone_number"),
                'source': check_object(i["person"], "source"),
                '_country': check_object(i["person"], "$country"),
                '_zip': check_object(i["person"], "$zip"),
                '_first_name': check_object(i["person"], "$first_name"),
                '_city': check_object(i["person"], "$city"),
                'adcopy': check_object(i["person"], "adcopy"),
                'email': check_object(i["person"], "email"),
                'welcome_popupDate_newsletter': check_object(i["person"], "Welcome-PopupDate-Newsletter"),
                'smile_points_balance': is_integer(check_object(i["person"], "Smile Points Balance")),
                '_consent_timestamp': check_object(i["person"], "$consent_timestamp"),
                'smile_state': check_object(i["person"], "Smile State"),
                'accepts_marketing': is_bool(check_object(i["person"], "Accepts Marketing")),
                'smile_referral_url': check_object(i["person"], "Smile Referral URL"),
                'expected_date_of_next_order': check_object(i["person"], "Expected Date Of Next Order"),
                '_consent_method': check_object(i["person"], "$consent_method"),
                '_consent_form_id': check_object(i["person"], "$consent_form_id"),
                '_consent_form_version': is_integer(check_object(i["person"], "$consent_form_version")),
                'welcome_popup': check_object(i["person"], "Welcome-Popup"),
                '_source': check_object(i["person"], "$source"),
                'last_placed_order_item3_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item3 Name (OCU)"),
                'last_placed_order_image3_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image3 URLs (OCU)"),
                'last_placed_order_price4_ocu': check_object(i["person"], "Last Placed Order Price4 (OCU)"),
                'last_placed_order_quantity4_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity4 (OCU)"),
                'last_placed_order_item2_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item2 Name (OCU)"),
                'last_placed_order_image1_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image1 URLs (OCU)"),
                'last_abandoned_cart_image3_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image3 URLs (OCU)"),
                'last_abandoned_cart_price2_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price2 (OCU)"),
                'last_placed_order_image5_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image5 URLs (OCU)"),
                'last_placed_order_cart_subtotal_ocu': check_object(i["person"],
                                                                    "Last Placed Order Cart Subtotal (OCU)"),
                'last_abandoned_cart_item3_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item3 Names (OCU)"),
                'last_placed_order_price3_ocu': check_object(i["person"], "Last Placed Order Price3 (OCU)"),
                'last_abandoned_cart_image2_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image2 URLs (OCU)"),
                'last_placed_order_item5_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item5 Name (OCU)"),
                'last_abandoned_cart_image4_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image4 URLs (OCU)"),
                'last_abandoned_cart_item2_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item2 Names (OCU)"),
                'last_placed_order_price2_ocu': check_object(i["person"], "Last Placed Order Price2 (OCU)"),
                'last_abandoned_cart_subtotal_ocu': check_object(i["person"],
                                                                 "Last Abandoned Cart Subtotal (OCU)"),
                'last_abandoned_cart_quantity2_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity2 (OCU)"),
                'last_abandoned_cart_price1_ocu': is_float(
                    check_object(i["person"], "Last Abandoned Cart Price1 (OCU)")),
                'last_placed_order_item4_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item4 Name (OCU)"),
                'last_abandoned_cart_image5_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image5 URLs (OCU)"),
                'last_abandoned_cart_quantity4_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity4 (OCU)"),
                'last_abandoned_cart_quantity1_ocu': is_integer(check_object(i["person"],
                                                                             "Last Abandoned Cart Quantity1 (OCU)")),
                'last_abandoned_cart_date_ocu': check_object(i["person"], "Last Abandoned Cart Date (OCU)"),
                'accepts_marketing_ocu': check_object(i["person"], "Accepts Marketing (OCU)"),
                'last_abandoned_cart_quantity3_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity3 (OCU)"),
                'last_abandoned_cart_quantity5_ocu': check_object(i["person"],
                                                                  "Last Abandoned Cart Quantity5 (OCU)"),
                'last_placed_order_item1_name_ocu': check_object(i["person"],
                                                                 "Last Placed Order Item1 Name (OCU)"),
                'last_abandoned_cart_item5_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item5 Names (OCU)"),
                'last_abandoned_cart_restore_url_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Restore URL (OCU)"),
                'last_placed_order_price1_ocu': is_float(
                    check_object(i["person"], "Last Placed Order Price1 (OCU)")),
                'last_abandoned_cart_item1_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item1 Names (OCU)"),
                'phone_number_ocu': check_object(i["person"], "Phone Number (OCU)"),
                'last_placed_order_date_ocu': check_object(i["person"], "Last Placed Order Date (OCU)"),
                'last_abandoned_cart_price4_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price4 (OCU)"),
                'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity2 (OCU)"),
                'last_abandoned_cart_price5_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price5 (OCU)"),
                'last_abandoned_cart_item4_names_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Item4 Names (OCU)"),
                'last_placed_order_image2_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image2 URLs (OCU)"),
                'last_placed_order_image4_urls_ocu': check_object(i["person"],
                                                                  "Last Placed Order Image4 URLs (OCU)"),
                'last_placed_order_quantity1_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity1 (OCU)"),
                'last_placed_order_quantity5_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity5 (OCU)"),
                'last_abandoned_cart_price3_ocu': check_object(i["person"],
                                                               "Last Abandoned Cart Price3 (OCU)"),
                'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                "Last Placed Order Quantity3 (OCU)"),
                'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                    "Last Abandoned Cart Image1 URLs (OCU)"),
                'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                'utm_content': check_object(i["person"], "UTM Content"),
                'last_purchased_offer_date_ocu': check_object(i["person"],
                                                              "Last Purchased Offer Date (OCU)"),
                'last_purchased_offer_price_ocu': is_float(
                    check_object(i["person"], "Last Purchased Offer Price (OCU)")),
                'last_purchased_offer_item_name_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Item Name (OCU)"),
                'last_purchased_offer_cart_subtotal_ocu': check_object(i["person"],
                                                                       "Last Purchased Offer Cart Subtotal (OCU)"),
                'last_purchased_offer_image_url_ocu': check_object(i["person"],
                                                                   "Last Purchased Offer Image URL (OCU)"),
                'abandoned_cart_offer': check_object(i["person"], "AbandonedCartOffer"),
                'abandoned_cart_offer_date': check_object(i["person"], "AbandonedCartOfferDate"),
                'birthday': check_object(i["person"], "Birthday"),
                'ad_copy': check_object(i["person"], "ad_copy"),

                # '_consent': check_object(i["person"], "$consent"),
                # 'shopify_tags': check_object(i["person"], "Shopify Tags"),
                # 'recharge_subscriptions': check_object(i["person"], "ReCharge Subscriptions"),
            },
        'uuid': i["uuid"],
        'event_name': i["event_name"],
        'timestamp': int(i["timestamp"]),
        'object': i["object"],
        'datetime': i["datetime"],
        'statistic_id': check_object(i, "statistic_id"),
        'id': check_object(i, "id")
    }
    return item


def create_table(tablename):
    schema = [
        bigquery.SchemaField("uuid", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("object", "STRING"),
        bigquery.SchemaField("datetime", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("statistic_id", "STRING"),

        bigquery.SchemaField(
            "event_properties",
            "RECORD",
            fields=[
                # bigquery.SchemaField("email_domain", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_event_id", "STRING", mode="NULLABLE"),

                # bigquery.SchemaField("_cohort_message_send_cohort", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("_variation", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("_cohort_variation_send_cohort", "STRING", mode="NULLABLE"),
                #
                # bigquery.SchemaField("_message", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("_flow", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("subject", "STRING", mode="NULLABLE"),

                bigquery.SchemaField("list", "STRING", mode="NULLABLE"),

                # bigquery.SchemaField("client_name", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("client_canonical", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("client_os", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("_message_interaction", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("client_os_family", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("client_type", "STRING", mode="NULLABLE"),

                # bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
                # bigquery.SchemaField("bounce_type", "STRING", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "person",
            "RECORD",
            fields=[
                bigquery.SchemaField("updated", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_longitude", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("_email", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("object", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_latitude", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("_address1", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_address2", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_title", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_timezone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_organization", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_region", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("created", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_last_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_phone_number", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_country", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_zip", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_first_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_city", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("adcopy", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("welcome_popupDate_newsletter", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("smile_points_balance", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("_consent_timestamp", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("smile_state", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("accepts_marketing", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("smile_referral_url", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("expected_date_of_next_order", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_consent_method", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_consent_form_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_consent_form_version", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("welcome_popup", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_source", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_item3_name_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_image3_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_price4_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_quantity4_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_item2_name_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_image1_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_image3_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_price2_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_image5_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_cart_subtotal_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_item3_names_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_price3_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_image2_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_item5_name_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_image4_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_item2_names_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_price2_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_subtotal_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_quantity2_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_price1_ocu", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_item4_name_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_image5_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_quantity4_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_quantity1_ocu", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_date_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("accepts_marketing_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_quantity3_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_quantity5_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_item1_name_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_item5_names_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_restore_url_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_price1_ocu", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_item1_names_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("phone_number_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_date_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_price4_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_quantity2_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_price5_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_item4_names_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_image2_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_image4_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_quantity1_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_quantity5_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_price3_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_quantity3_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_abandoned_cart_image1_urls_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_placed_order_price5_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("welcome_non_newsletter", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("welcome_popup_date", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("utm_content", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_purchased_offer_date_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_purchased_offer_price_ocu", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("last_purchased_offer_item_name_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_purchased_offer_cart_subtotal_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_purchased_offer_image_url_ocu", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("abandoned_cart_offer", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("abandoned_cart_offer_date", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("birthday", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("ad_copy", "STRING", mode="NULLABLE"),
            ],
        ),
    ]

    tid = project_id + '.' + dataset + '.' + tablename
    print("Created table " + tid)

    t = bigquery.Table(tid, schema=schema)
    table = bigquery_client.create_table(t)  # API request
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


def tbl_exists(client, table_ref):
    from google.cloud.exceptions import NotFound
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def call_api(request):
    # def call_api(event, context):
    metrics = client.Metrics.get_metrics(count=100)
    r = json.dumps(metrics.data["data"])
    loaded_r = json.loads(r)

    today = str(datetime.now().date()).replace("-", "/")
    tomorrow = str(datetime.now().date() + timedelta(days=1)).replace("-", "/")
    # get_metric_data("MSUv2N", "2021/04/11", "2021/04/10", "2021/04/11", "receive", table)

    for metric in loaded_r:
        if metric["id"] == "LfJBNg":
            table_ref = dataset_ref.table("bounce")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("bounce")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("LfJBNg", tomorrow, today, tomorrow, "bounce", table)

        if metric["id"] == "Ky6Pf6":
            table_ref = dataset_ref.table("click")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("click")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("Ky6Pf6", tomorrow, today, tomorrow, "click", table)

        if metric["id"] == "PQAnPe":
            table_ref = dataset_ref.table("dropped_email")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("dropped_email")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("PQAnPe", tomorrow, today, tomorrow, "dropped_email", table)

        if metric["id"] == "LSKEqT":
            table_ref = dataset_ref.table("mark_as_spam")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("mark_as_spam")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("LSKEqT", tomorrow, today, tomorrow, "mark_as_spam", table)

        if metric["id"] == "PFknxh":
            table_ref = dataset_ref.table("open")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("open")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("PFknxh", tomorrow, today, tomorrow, "open", table)

        if metric["id"] == "MSUv2N":
            table_ref = dataset_ref.table("receive")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("receive")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("MSUv2N", tomorrow, today, tomorrow, "receive", table)

        if metric["id"] == "NuiTPX":
            table_ref = dataset_ref.table("subscribe_list")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("subscribe_list")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("NuiTPX", tomorrow, today, tomorrow, "subscribe_list", table)

        if metric["id"] == "KrUvFp":
            table_ref = dataset_ref.table("unsub_list")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("unsub_list")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("KrUvFp", tomorrow, today, tomorrow, "unsub_list", table)

        if metric["id"] == "JtiJhM":
            table_ref = dataset_ref.table("unsubscribe")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("unsubscribe")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("JtiJhM", tomorrow, today, tomorrow, "unsubscribe", table)

        if metric["id"] == "Q3E8mF":
            table_ref = dataset_ref.table("update_email_preferences")
            if not tbl_exists(bigquery_client, table_ref):
                create_table("update_email_preferences")
            table = bigquery_client.get_table(table_ref)
            get_metric_data("Q3E8mF", tomorrow, today, tomorrow, "update_email_preferences", table)

    # get_metric_data("K8vC8L", "2021/01/31", "2021/01/01", "2021/01/31", "bounce")
    # get_next_data("2021/03/23", "2021/03/23", "2021/03/20", "click")
    # get_next_data("2021/03/23", "2021/03/23", "2021/03/20", "open")
    # get_next_data("2021/03/23", "2021/03/23", "2021/03/20", "dropped_email")
    # get_next_data("2021/03/23", "2021/03/23", "2021/03/20", "dropped_email")

    return "done"


if __name__ == '__main__':
    call_api("request")
