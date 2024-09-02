from google.oauth2 import service_account

import functions_framework
import pandas as pd
import requests
import datetime
import json
import pandas_gbq

END_DATE = datetime.datetime.today().replace(month=12, day=31).strftime('%Y-%m-%d')
KEY_PATH = "./cloud_run_functions/my_service_account_credentials.json"
TABLE_SCHEMA_PATH = "./table_schema.json"
BIGQUERY_TABLE_ID = "..."  # table_id
URL = f"https://openholidaysapi.org/SchoolHolidays?countryIsoCode=NL&languageIsoCode=EN&validFrom=2023-01-01&validTo={END_DATE}&subdivisionCode=NL-MI"
SOURCE = "open_holidays"


def events_to_dataframe(url: str, source: str):
    """gather school holidays data using the OpenHolidaysAPI and transform to a dataframe"""

    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    school_holiday = json.loads(response.text)

    holiday_data = []
    for holiday in school_holiday:
        holiday_info = {
            'name_holiday': holiday.get('name'),
            'start_date': holiday.get('startDate'),
            'end_date': holiday.get('endDate'),
            'holiday_type': holiday.get('type'),
            'source': source
        }

        for text in holiday_info['name_holiday']:
            holiday_info['name_holiday'] = text['text']

        holiday_data.append(holiday_info)

    df = pd.DataFrame(holiday_data)

    return df


def dataframe_to_bigquery(
        df: pd.DataFrame,
        bigquery_table_id: str,
        key_path: str,
        schema_path: str,
        bigquery_dataset_id: str = "ichiban_data_raw",
        project_id_gc: str = "185701563519",
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

    df = events_to_dataframe(url=URL, source=SOURCE)

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
    return f'{name} lock and loaded!'
