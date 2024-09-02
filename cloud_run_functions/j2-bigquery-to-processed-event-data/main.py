from google.oauth2 import service_account

import functions_framework
import pandas as pd
import json
import pandas_gbq as pdg

KEY_PATH = "./cloud_run_functions/my_service_account_credentials.json"
TABLE_SCHEMA_PATH = "./table_schema.json"
BIGQUERY_TABLE_ID = "..."  # table_id

# processed SQL queries
processed_events_query = "./processed_events.sql"


def read_bigquery_to_pandas_df(
        key_path: str,
        query: str,
        project_id: str = "...",
) -> pd.DataFrame:
    """Read data via BigQuery in a Pandas dataframe"""

    creds = service_account.Credentials.from_service_account_file(KEY_PATH)
    sql = open(query, "r").read()

    df = pdg.read_gbq(
        query_or_table=sql,
        project_id=project_id,
        location="europe-west4",
        credentials=creds
    )

    return df


def dataframe_to_bigquery(
        df: pd.DataFrame,
        bigquery_table_id: str,
        key_path: str,
        schema_path: str,
        bigquery_dataset_id: str = "...",
        project_id_gc: str = "...",
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

    # perform the query for retrieving all data
    df_events = read_bigquery_to_pandas_df(key_path=KEY_PATH, query=processed_events_query)

    # ingestion of the processed events data to BigQuery
    dataframe_to_bigquery(
        df=df_events,
        bigquery_table_id=BIGQUERY_TABLE_ID,
        key_path=KEY_PATH,
        schema_path=TABLE_SCHEMA_PATH,
    )

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'Ichiban'
    return '{} lock and processed!'.format(name)
