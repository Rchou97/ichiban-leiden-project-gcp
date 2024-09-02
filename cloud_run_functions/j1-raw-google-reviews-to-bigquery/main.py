from google.oauth2 import service_account
from serpapi import GoogleSearch
from urllib.parse import urlsplit, parse_qsl

import functions_framework
import pandas as pd
import json
import pandas_gbq

KEY_PATH = "./cloud_run_functions/my_service_account_credentials.json"
TABLE_SCHEMA_PATH = "./table_schema.json"
BIGQUERY_TABLE_ID = "google_reviews"  # table_id
PARAMS = {
    "api_key": "...",
    "engine": "google_maps_reviews",
    "hl": "en",
    "data_id": "...:...",
    "sort_by": "Newest",
}


def reviews_to_dataframe(params: dict):
    """gather Google reviews from the applicable place_id via the Google Maps API"""

    search = GoogleSearch(params)
    source = "google_reviews"
    page_num = 1
    results = search.get_dict()

    print(f"Extracting reviews from {page_num} page.")
    if 'reviews' in results:
        reviews_data = []
        for review in results['reviews']:
            review_data = {
                'date': review.get("iso_date"),
                'author': review.get("user").get("name"),
                'rating': review.get('rating'),
                'review': review.get("snippet"),
                'local_guide': review.get("user").get("local_guide"),
                'details': review.get("details"),
                'link': review.get('user').get('link'),
                'source': source,
                'page': page_num,
            }
            reviews_data.append(review_data)

        while True:
            try:
                results.get("serpapi_pagination").get("next") and results.get("serpapi_pagination").get(
                    "next_page_token")
                page_num += 1
                print(f"Extracting reviews from {page_num} page.")

                search.params_dict.update(dict(parse_qsl(urlsplit(results["serpapi_pagination"]["next"]).query)))

                results = search.get_dict()

                if 'reviews' in results:
                    for review in results['reviews']:
                        review_data = {
                            'date': review.get("iso_date"),
                            'author': review.get("user").get("name"),
                            'rating': review.get('rating'),
                            'review': review.get("snippet"),
                            'local_guide': review.get("user").get("local_guide"),
                            'details': review.get("details"),
                            'link': review.get('user').get('link'),
                            'source': source,
                            'page': page_num,
                        }
                        reviews_data.append(review_data)
            except AttributeError:
                break

        df = pd.DataFrame(reviews_data)
    else:
        print("No reviews found.")
        return None

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

    df = reviews_to_dataframe(params=PARAMS)
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    print(df.head())

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
