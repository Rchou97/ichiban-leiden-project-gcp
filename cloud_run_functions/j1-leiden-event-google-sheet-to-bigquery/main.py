from google.oauth2 import service_account

import functions_framework
import pandas as pd
import gspread
import json
import pandas_gbq


KEY_PATH = "./cloud_run_functions/my_service_account_credentials.json"
TABLE_SCHEMA_PATH = "./table_schema.json"
BIGQUERY_TABLE_ID = "..."  # table_id
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
URL = "https://docs.google.com/spreadsheets/d/..."

def sheet_to_dataframe(key_path: str, gsheets_url: str) -> pd.DataFrame:
    """extract data from first sheet of google sheet"""

    with open(key_path) as file:
        credentials_json = json.load(file)

    credentials = service_account.Credentials.from_service_account_info(
        credentials_json
    )
    creds_with_scope = credentials.with_scopes(SCOPES)
    gspread_client = gspread.authorize(creds_with_scope)

    spreadsheet = gspread_client.open_by_url(gsheets_url)
    worksheet = spreadsheet.get_worksheet(0) # get tab 1
    records_data = worksheet.get_all_records()
    records_df = pd.DataFrame.from_dict(records_data)

    return records_df

def dataframe_to_bigquery(
    df: pd.DataFrame,
    bigquery_table_id: str,
    key_path: str,
    schema_path: str,
    bigquery_dataset_id: str = "...",
    project_id_gc: str = "...",
) -> None:
    """save dataframe to bigquery"""

    with open(schema_path, 'r') as f:
        schema = json.load(f)

    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f"{bigquery_dataset_id}.{bigquery_table_id}",
        project_id=project_id_gc,
        progress_bar=False,
        if_exists='replace',
        chunksize=None,
        api_method='load_csv',
        location='europe-west4', # BigQuery region Netherlands
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

    df = sheet_to_dataframe(key_path=KEY_PATH, gsheets_url=URL)

    dataframe_to_bigquery(
        df=df,
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
    return '{} lock and loaded!'.format(name)
