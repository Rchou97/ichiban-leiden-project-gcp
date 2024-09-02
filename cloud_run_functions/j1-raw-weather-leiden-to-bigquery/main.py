from google.oauth2 import service_account

import functions_framework
import pandas as pd
import requests
import datetime
import json
import pandas_gbq

KEY_PATH = "./cloud_run_functions/my_service_account_credentials.json"
TABLE_SCHEMA_PATH = "./table_schema.json"
BIGQUERY_TABLE_ID = "..."  # table_id
URL = "https://weerlive.nl/api/weerlive_api_v2.php?key=...&locatie=Leiden"
SOURCE = "weerlive"


def weather_data_to_dataframe(url: str, source: str):
    """gather weather data using the weerliveAPI and transform to a dataframe"""

    weer_headers = {"accept": "application/json"}
    response = requests.get(url, headers=weer_headers)
    weerlive_output = json.loads(response.text)

    weather_data = []

    for liveweer in weerlive_output['liveweer']:
        weer_info = {
            'date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'temp': float(liveweer.get('gtemp')),
            'windbft': int(liveweer.get('windbft')),
            'sun%': int(weerlive_output['wk_verw'][0].get('zond_perc_dag')),
            'rain%': int(weerlive_output['wk_verw'][0].get('neersl_perc_dag')),
            'expectedForecast': liveweer.get('verw'),
            'summary': liveweer.get('samenv'),
            'city': liveweer.get('plaats'),
            'source': source
        }

        weather_data.append(weer_info)

    df = pd.DataFrame(weather_data)

    return df


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
        if_exists='append',
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

    df = weather_data_to_dataframe(url=URL, source=SOURCE)

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
