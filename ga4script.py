import csv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import argparse
import datetime
import sys
import json

import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow

with open("config.json", "r") as f:
    config = json.load(f)

def exists_in_bigquery(event_name, event_date, event_count, channel_group, dataset_id, bq_client):

    year = event_date[:4]
    month = event_date[4:6]
    table_id = f'{TABLE_PREFIX}{year}{month}01'
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    try:
        bq_client.get_table(table_ref)
    except NotFound:

        return False

    query = """
        SELECT COUNT(*)
        FROM `{}.{}`
        WHERE `Event_Name` = @event_name
          AND `Event_Date` = @event_date
          AND `Event_Count` = @event_count
          AND `Channel` = @channel_group
    """.format(dataset_id, table_id)

    params = [
        bigquery.ScalarQueryParameter('event_name', 'STRING', event_name),
        bigquery.ScalarQueryParameter('event_date', 'INTEGER', event_date),
        bigquery.ScalarQueryParameter('event_count', 'INTEGER', event_count),
        bigquery.ScalarQueryParameter('channel_group', 'STRING', channel_group)
    ]

    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = params

    result = bq_client.query(query, job_config=job_config).result()
    count = list(result)[0][0]

    if count > 0:
        print(f"..record already exists in BigQuery ({count})")

    return count > 0

def get_table_ref(year, month):
    table_id = f'{TABLE_PREFIX}{year}{month}01'
    return bq_client.dataset(DATASET_ID).table(table_id)

CLIENT_SECRET_FILE = config['CLIENT_SECRET_FILE']
SCOPES = config['SCOPES']

TABLE_PREFIX = config['TABLE_PREFIX']
PROPERTY_ID = config['PROPERTY_ID']
DATASET_ID = config['DATASET_ID']
INITIAL_FETCH_FROM_DATE = config['INITIAL_FETCH_FROM_DATE']
SERVICE_ACCOUNT_FILE = config['SERVICE_ACCOUNT_FILE']

parser = argparse.ArgumentParser(description='Fetch data based on date range.')
parser.add_argument('--yesterday', action='store_true', help='Fetch data from yesterday only.')
parser.add_argument('--initial_fetch', action='store_true', help='Fetch data from a wide date range.')
args = parser.parse_args()

start_date = None
end_date = None

if args.yesterday:
    date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date = date.strftime('%Y-%m-%d')
elif args.initial_fetch:

    confirmation = input("Using the initial_fetch might result in duplicated records. Do you want to proceed? (yes/no): ").strip().lower()
    if confirmation == 'yes':
        start_date = INITIAL_FETCH_FROM_DATE
        end_date = datetime.date.today().strftime('%Y-%m-%d')
    else:
        print("Exiting script due to user cancellation.")
        sys.exit()
else:
    print("No valid date range argument provided. Exiting script.")
    sys.exit()

print(f"Starting fetching data from {start_date} to {end_date}.")

creds1 = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=['https://www.googleapis.com/auth/analytics.readonly', 'https://www.googleapis.com/auth/bigquery']
)
bq_client = bigquery.Client(credentials=creds1, project=creds1.project_id)

if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
else:

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
    creds = flow.run_local_server(port=8080)

    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

print("Authentication successful!")

with open('token.pickle', 'rb') as token:
    creds = pickle.load(token)

client = BetaAnalyticsDataClient(credentials=creds)

request_active_users = RunReportRequest(
    property=f'properties/{PROPERTY_ID}',
    date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    dimensions=[Dimension(name='date')],
    metrics=[Metric(name='activeUsers')],
    dimension_filter=None
)

response_active_users = client.run_report(request_active_users)

request_events = RunReportRequest(
    property=f'properties/{PROPERTY_ID}',
    date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    dimensions=[Dimension(name='eventName'), Dimension(name='date'), Dimension(name='isConversionEvent'), Dimension(name='sessionDefaultChannelGroup')],
    metrics=[Metric(name='eventCount')]
)

response_events = client.run_report(request_events)

sorted_active_users = sorted(response_active_users.rows, key=lambda x: x.dimension_values[0].value)

sorted_events = sorted(response_events.rows, key=lambda x: x.dimension_values[1].value)

rows_by_month = {}

with open('output.csv', 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile)

    csv_writer.writerow(['Event Name', 'Event Date', 'Event Count', 'Is Conversion', 'Channel'])

    for row in sorted_active_users:
        event_name = "ct_active_users"
        is_conversion = None
        event_date = row.dimension_values[0].value
        event_count = row.metric_values[0].value
        channel_group = ''

        csv_writer.writerow([event_name, event_date, event_count, is_conversion, ''])

        if args.yesterday and exists_in_bigquery(event_name, event_date, event_count, channel_group, DATASET_ID, bq_client):

            pass
        else:

            year = event_date[:4]
            month = event_date[4:6]
            key = (year, month)

            if key not in rows_by_month:
                rows_by_month[key] = []

            rows_by_month[key].append({
                "Event_Name": event_name,
                "Event_Date": event_date,
                "Event_Count": event_count,
                "Is_Conversion": is_conversion,
                "Channel": channel_group
            })

    for row in sorted_events:
        event_name = row.dimension_values[0].value
        event_date = row.dimension_values[1].value
        is_conversion = row.dimension_values[2].value

        if is_conversion == "(not set)":
            is_conversion = ""

        channel_group = row.dimension_values[3].value
        event_count = row.metric_values[0].value

        is_conversion = bool(is_conversion)

        if is_conversion:
            csv_writer.writerow([event_name, event_date, event_count, is_conversion, channel_group])

        if is_conversion:
           if args.yesterday and exists_in_bigquery(event_name, event_date, event_count, channel_group, DATASET_ID, bq_client):

               pass
           else:
                is_conversion = bool(is_conversion)

                year = event_date[:4]
                month = event_date[4:6]
                key = (year, month)

                if key not in rows_by_month:
                    rows_by_month[key] = []

                rows_by_month[key].append({
                    "Event_Name": event_name,
                    "Event_Date": event_date,
                    "Event_Count": event_count,
                    "Is_Conversion": is_conversion,
                    "Channel": channel_group
                })

print("Data saved to output.csv!")

schema = [
    bigquery.SchemaField("Event_Name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Event_Date", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Event_Count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Is_Conversion", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("Channel", "STRING", mode="NULLABLE")
]

for (year, month), rows_to_insert in rows_by_month.items():
    table_ref = get_table_ref(year, month)

    try:
        bq_client.get_table(table_ref)
    except NotFound:

        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Table {table.table_id} created.")

    errors = bq_client.insert_rows(table_ref, rows_to_insert, selected_fields=schema)
    if errors:
        print("Errors:", errors)
    else:
        print(f"Data saved to BigQuery for {month}/{year}!")
