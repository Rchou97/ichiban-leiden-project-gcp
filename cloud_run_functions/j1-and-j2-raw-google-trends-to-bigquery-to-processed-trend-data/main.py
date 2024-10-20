from google.oauth2 import service_account
from google.cloud import bigquery
from serpapi import GoogleSearch
from datetime import date

import functions_framework
import pandas as pd
import datetime
import json
import os
import pandas_gbq as pdg

TODAY = date.today()
KEY_PATH = "./my_service_account_credentials.json"
TABLE_SCHEMA_PATH_RAW = "./table_schema_raw.json"
TABLE_SCHEMA_PATH_PROCESSED = "./table_schema_processed.json"
BIGQUERY_TABLE_ID = "google_trends"  # table_id
PARAMS = {
    "api_key": "ba176ec5b477733df26534353f32ac92d1f86e9449a6aba075214804b124b44b",
    "engine": "google_trends",
    "q": "sushi leiden",
    "data_type": "TIMESERIES",
    "date": f"2023-05-28 {TODAY}",
}

# processed SQL queries
processed_trends_query = "./processed_trends.sql"


def trends_to_dataframe(params: dict):
    """gather Google Trends from the applicable keywords via the Google Trends API"""

    search = GoogleSearch(params)
    source = "google_trends"
    results = search.get_dict()
    interest_over_time = results["interest_over_time"]
    trend_results = []

    print(f"Extracting trends from SerpAPI.")
    for result in interest_over_time["timeline_data"]:
        for value in result["values"]:
            trends_data = {
                'date': result["date"],
                'timestamp': result["timestamp"],
                'query': value["query"],
                'value': value["value"],
                'extracted_value': value["extracted_value"],
                'source': source,
            }
            trend_results.append(trends_data)

    df = pd.DataFrame(trend_results)

    return df


def dataframe_to_bigquery(
        df: pd.DataFrame,
        bigquery_table_id: str,
        key_path: str,
        schema_path: str,
        bigquery_dataset_id: str,
        project_id_gc: str = "185701563519",
        load_type: str = "load_parquet",
) -> None:
    """save dataframe to bigquery"""

    with open(schema_path, 'r') as f:
        schema = json.load(f)

    pdg.to_gbq(
        dataframe=df,
        destination_table=f"{bigquery_dataset_id}.{bigquery_table_id}",
        project_id=project_id_gc,
        progress_bar=False,
        if_exists='replace',
        chunksize=None,
        api_method=load_type,
        location='europe-west4',  # BigQuery region Netherlands
        credentials=service_account.Credentials.from_service_account_file(key_path),
        table_schema=schema,
        verbose=False,
    )
    print(f"Dataframe saved. Numbers of rows loaded: {len(df)}")


def read_bigquery_to_pandas_df(
        key_path: str,
        query_or_table: str,
        project_id: str = "gcp-ichiban-data-ingestion",
) -> pd.DataFrame:
    """Read data via BigQuery in a Pandas dataframe via SQL query or table name"""

    creds = service_account.Credentials.from_service_account_file(key_path)

    if query_or_table.endswith('.sql'):
        query_or_table = open(query_or_table, "r").read()

    df = pdg.read_gbq(
        query_or_table=query_or_table,
        project_id=project_id,
        location="europe-west4",
        credentials=creds
    )

    return df


@functions_framework.http
def main(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    # Load modified Raw table to Bigquery
    # Extract trends from API
    df_raw = trends_to_dataframe(params=PARAMS)
    print(df_raw.head())

    dataframe_to_bigquery(
        df=df_raw,
        bigquery_table_id=BIGQUERY_TABLE_ID,
        key_path=KEY_PATH,
        schema_path=TABLE_SCHEMA_PATH_RAW,
        bigquery_dataset_id='ichiban_data_raw',
        load_type='load_csv'
    )

    # j2 Raw table to Processed layer
    # Perform the query for retrieving all data
    df_processed = read_bigquery_to_pandas_df(
        key_path=KEY_PATH,
        query_or_table=processed_trends_query
    )

    # Load Processed table to Bigquery
    dataframe_to_bigquery(
        df=df_processed,
        bigquery_table_id=BIGQUERY_TABLE_ID,
        key_path=KEY_PATH,
        schema_path=TABLE_SCHEMA_PATH_PROCESSED,
        bigquery_dataset_id='ichiban_data_processed'
    )

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'Ichiban'
    return f'{name} lock and loaded!'
