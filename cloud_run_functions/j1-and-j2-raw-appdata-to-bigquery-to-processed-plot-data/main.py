from google.oauth2 import service_account
from google.cloud import bigquery
from google.auth.transport.requests import AuthorizedSession

import functions_framework
import pandas as pd
import json
import os
import pandas_gbq as pdg

KEY_PATH = "./my_service_account_credentials.json"
TABLE_SCHEMA_PATH_RAW = "./table_schema_raw.json"
TABLE_SCHEMA_PATH_PROCESSED = "./table_schema_processed.json"
BIGQUERY_TABLE = "app_source.ichiban_revenue"
BIGQUERY_TABLE_ID_RAW = "ichiban_revenue_appdata"  # table_id_raw
BIGQUERY_TABLE_ID_PROCESSED = "ichiban_revenue"  # table_id_processed

# processed SQL queries
processed_revenue_query = "./processed_revenue.sql"


def read_bigquery_to_pandas_df(
        key_path: str,
        query_or_table: str,
        project_id: str = "",
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


def raw_to_processed(df: pd.DataFrame) -> pd.DataFrame:
    """clean data to processed format for plotting"""

    # Copy 'Comm' column
    column_comm = df['Comm']

    # New processed column creation
    df['Tijd'] = pd.to_datetime(df['Tijd'])
    df[df.columns.difference(['Tijd'])] = df[df.columns.difference(['Tijd'])].apply(pd.to_numeric, errors='coerce')
    df['day'] = df['Tijd'].dt.strftime('%A')
    df['week_n'] = df['Tijd'].dt.isocalendar().week
    df['month'] = df['Tijd'].dt.strftime('%B')
    df['dayOfMonth'] = df['Tijd'].dt.day
    df['dateForm'] = df['Tijd'].dt.strftime('%Y-%m-%d')

    # Final dataframe modifications
    df = df.dropna(subset=['total_am'])[(df['total_am'] != 0)]
    df.fillna(0, inplace=True)

    # Drop "NaN" 'Comm' column and merge it with copied column 'Comm'
    df = df.drop(columns=['Comm'])
    df = pd.merge(df, column_comm, left_index=True, right_index=True)

    return df


def dataframe_to_bigquery(
        df: pd.DataFrame,
        bigquery_table_id: str,
        key_path: str,
        schema_path: str,
        bigquery_dataset_id: str,
        project_id_gc: str = "",
) -> None:
    """Save dataframe to bigquery"""

    with open(schema_path, 'r') as f:
        schema = json.load(f)

    pdg.to_gbq(
        dataframe=df,
        destination_table=f"{bigquery_dataset_id}.{bigquery_table_id}",
        project_id=project_id_gc,
        progress_bar=False,
        if_exists='replace',
        chunksize=None,
        api_method='load_parquet',
        location='europe-west4',  # BigQuery region Netherlands
        credentials=service_account.Credentials.from_service_account_file(key_path),
        table_schema=schema,
        verbose=False,
    )
    print(f"Dataframe saved. Numbers of rows loaded: {len(df)}")


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

    # j1 Appdata to Raw layer
    # Extract data from forms
    df_app_data = read_bigquery_to_pandas_df(
        key_path=KEY_PATH,
        query_or_table=BIGQUERY_TABLE
    )

    # Cast types to appropriate data types
    df_app_data = df_app_data.astype({
        'Tijd': 'datetime64[ns]',
        'pin': 'float64',
        'n_pin': 'int64',
        'cash': 'float64',
        'n_cash': 'int64',
        'ideal': 'float64',
        'n_ideal': 'int64',
        'thuis': 'float64',
        'n_thuis': 'int64',
    })

    # Calculate new columns
    df_app_data['total_am'] = round(
        df_app_data['pin'] + df_app_data['cash'] + df_app_data['ideal'] + df_app_data['thuis'], 2)
    df_app_data['n_total'] = df_app_data['n_pin'] + df_app_data['n_cash'] + df_app_data['n_ideal'] + df_app_data[
        'n_thuis']

    df_app_data['avg_pin'] = round(df_app_data['pin'] / df_app_data['n_pin'], 2)
    df_app_data['avg_cash'] = round(df_app_data['cash'] / df_app_data['n_cash'], 2)
    df_app_data['avg_ideal'] = round(df_app_data['ideal'] / df_app_data['n_ideal'], 2)
    df_app_data['avg_thuis'] = round(df_app_data['thuis'] / df_app_data['n_thuis'], 2)
    df_app_data['avg_total'] = round(df_app_data['total_am'] / df_app_data['n_total'], 2)

    df_app_data['%N_pin'] = round(df_app_data['n_pin'] / df_app_data['n_total'], 2)
    df_app_data['%N_cash'] = round(df_app_data['n_cash'] / df_app_data['n_total'], 2)
    df_app_data['%N_ideal'] = round(df_app_data['n_ideal'] / df_app_data['n_total'], 2)
    df_app_data['%N_thuis'] = round(df_app_data['n_thuis'] / df_app_data['n_total'], 2)

    df_app_data['%am_pin'] = round(df_app_data['pin'] / df_app_data['total_am'], 2)
    df_app_data['%am_cash'] = round(df_app_data['cash'] / df_app_data['total_am'], 2)
    df_app_data['%am_ideal'] = round(df_app_data['ideal'] / df_app_data['total_am'], 2)
    df_app_data['%am_thuis'] = round(df_app_data['thuis'] / df_app_data['total_am'], 2)

    # Load modified Raw table to Bigquery
    dataframe_to_bigquery(
        df=df_app_data,
        bigquery_table_id=BIGQUERY_TABLE_ID_RAW,
        key_path=KEY_PATH,
        schema_path=TABLE_SCHEMA_PATH_RAW,
        bigquery_dataset_id='ichiban_data_raw'
    )

    # j2 Raw table to Processed layer
    # Perform the query for retrieving all data
    df_revenue = read_bigquery_to_pandas_df(
        key_path=KEY_PATH,
        query_or_table=processed_revenue_query
    )

    # Perform cleaning before send to Processed layer
    processed_df_revenue = raw_to_processed(df_revenue)

    # Load modified Processed table to Bigquery
    dataframe_to_bigquery(
        df=processed_df_revenue,
        bigquery_table_id=BIGQUERY_TABLE_ID_PROCESSED,
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
    return '{} lock and loaded!'.format(name)
