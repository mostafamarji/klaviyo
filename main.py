import json
import os
import time
from collections import OrderedDict

import klaviyo
from google.cloud import bigquery

project_id = 'sugatan-290314'
dataset = 'Klaviyo'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sugatan-290314-c08f69990c42.json"

bigquery_client = bigquery.Client()
dataset_ref = bigquery_client.dataset(dataset)

klaviyo_public_token = "Np5mQ8"
klaviyo_private_token = "pk_e0d671013192332ff5a2cf1519bcb92d70"
client = klaviyo.Klaviyo(public_token=klaviyo_public_token, private_token=klaviyo_private_token)

final_lst = []
ordered_person_list = []


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


def get_next_data(sincedata, requestname):
    # try:
    table_ref = dataset_ref.table(requestname)
    table = bigquery_client.get_table(table_ref)

    # bounce_email
    metrics = client.Metrics.get_metric_timeline_by_id("K8vC8L", since=sincedata, sort="desc")

    # click_email
    # metrics = client.Metrics.get_metric_timeline_by_id("MMHziA", since=sincedata)

    r = json.dumps(metrics.data["data"])
    loaded_r = json.loads(r)
    print(loaded_r)
    # save_click_data_big_query(loaded_r, table)
    # save_bounce_data_big_query(loaded_r, table)
    # get_table_properties(loaded_r)

    if metrics.data["next"] != "":
        time.sleep(3)
        print("Getting next data")
        get_next_data(metrics.data["next"], requestname)
    else:
        return


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
        for att in o['person']:
            person_list.append(att)
    result = list(OrderedDict.fromkeys(person_list))
    for j in result:
        final_lst.append(j)
    result = list(OrderedDict.fromkeys(final_lst))
    print(result)


def save_click_data_big_query(loaded_r, table):
    try:
        for i in loaded_r:
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
                        'message': check_object(i["event_properties"], "$message"),
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
                        'last_abandoned_cart_price2_ocu': check_object(i["person"], "Last Abandoned Cart Price2 (OCU)"),
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
                        'last_abandoned_cart_price4_ocu': check_object(i["person"], "Last Abandoned Cart Price4 (OCU)"),
                        'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                        "Last Placed Order Quantity2 (OCU)"),
                        'last_abandoned_cart_price5_ocu': check_object(i["person"], "Last Abandoned Cart Price5 (OCU)"),
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
                        'last_abandoned_cart_price3_ocu': check_object(i["person"], "Last Abandoned Cart Price3 (OCU)"),
                        'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                        "Last Placed Order Quantity3 (OCU)"),
                        'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                            "Last Abandoned Cart Image1 URLs (OCU)"),
                        'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                        'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                        'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                        'utm_content': check_object(i["person"], "UTM Content"),
                        'last_purchased_offer_date_ocu': check_object(i["person"], "Last Purchased Offer Date (OCU)"),
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
            final_lst.append(item)
        result = bigquery_client.insert_rows_json(table, final_lst)
        print(result)
        # final_lst.append(item)
        # bigquery_client.insert_rows_json(table, final_lst)
    except Exception as e:
        print(e)


def save_bounce_data_big_query(loaded_r, table):
    try:
        for i in loaded_r:
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
                        '_email_domain': check_object(i["event_properties"], "Email Domain"),
                        '_bounce_type': check_object(i["event_properties"], "Bounce Type"),

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
                        'last_abandoned_cart_price2_ocu': check_object(i["person"], "Last Abandoned Cart Price2 (OCU)"),
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
                        'last_abandoned_cart_price4_ocu': check_object(i["person"], "Last Abandoned Cart Price4 (OCU)"),
                        'last_placed_order_quantity2_ocu': check_object(i["person"],
                                                                        "Last Placed Order Quantity2 (OCU)"),
                        'last_abandoned_cart_price5_ocu': check_object(i["person"], "Last Abandoned Cart Price5 (OCU)"),
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
                        'last_abandoned_cart_price3_ocu': check_object(i["person"], "Last Abandoned Cart Price3 (OCU)"),
                        'last_placed_order_quantity3_ocu': check_object(i["person"],
                                                                        "Last Placed Order Quantity3 (OCU)"),
                        'last_abandoned_cart_image1_urls_ocu': check_object(i["person"],
                                                                            "Last Abandoned Cart Image1 URLs (OCU)"),
                        'last_placed_order_price5_ocu': check_object(i["person"], "Last Placed Order Price5 (OCU)"),
                        'welcome_non_newsletter': check_object(i["person"], "Welcome-Non-Newsletter"),
                        'welcome_popup_date': check_object(i["person"], "Welcome-PopupDate"),
                        'utm_content': check_object(i["person"], "UTM Content"),
                        'last_purchased_offer_date_ocu': check_object(i["person"], "Last Purchased Offer Date (OCU)"),
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
            final_lst.append(item)
        result = bigquery_client.insert_rows_json(table, final_lst)
        print(result)
        # final_lst.append(item)
        # bigquery_client.insert_rows_json(table, final_lst)
    except Exception as e:
        print(e)


def call_api(requestName):
    # if requestName == "bounce":

    get_next_data("2021/03/20", "bounce")
    # get_next_data("3/19/2021", "bounce")

    return "done"


if __name__ == '__main__':
    call_api('bounce')
