from google.oauth2 import service_account
from deep_translator import GoogleTranslator
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import functions_framework
import pandas as pd
import json
import pandas_gbq as pdq

KEY_PATH = "./cloud_run_functions/my_service_account_credentials.json"
TABLE_SCHEMA_PATH = "./table_schema.json"
BIGQUERY_TABLE_ID = "..."  # table_id

# processed SQL queries
processed_reviews_query = "./processed_reviews.sql"

# initialise SentimentIntensityAnalyzer
sentiment = SentimentIntensityAnalyzer()

def google_translator(text: str, language: str) -> None:
    """Translate text to Mandarin"""
    if text is None:
        return None
    else:
        try:
            translated_text = GoogleTranslator(source='auto', target=language).translate(text)
            return translated_text
        except Exception as e:
            print(f"ERROR - Error translating text: {e}")
            pass


def get_sentiment(text: str) -> float:
    """Calculate the sentiment of the text"""

    if text is None or not isinstance(text, str):
        return None
    else:
        try:
            sentiment_score = sentiment.polarity_scores(text)["compound"]
            return sentiment_score
        except Exception as e:
            print(f"ERROR - Error translating text: {e}")
            return 0


def read_bigquery_to_pandas_df(
        key_path: str,
        query_or_table: str,
        project_id: str = "...",
) -> pd.DataFrame:
    """Read data via BigQuery in a Pandas dataframe"""

    creds = service_account.Credentials.from_service_account_file(key_path)
    sql = open(query_or_table, "r").read()

    df = pdq.read_gbq(
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

    pdq.to_gbq(
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
    print(f"INFO - Dataframe saved. Numbers of rows loaded: {len(df)}")


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
    df_reviews = read_bigquery_to_pandas_df(
        key_path=KEY_PATH,
        query_or_table=processed_reviews_query
    )

    # translate review to Mandarin and English
    print("INFO - Translating the reviews to Mandarin and English (where necessary)")
    df_reviews['review_zh'] = df_reviews['review'].apply(google_translator, language='zh-CN')
    df_reviews['review_en'] = df_reviews['review'].apply(google_translator, language='en')

    # calculate sentiment
    print("INFO - Translation done. Calculate sentiment scores for English reviews")
    df_reviews['sentiment_score_en'] = df_reviews['review_en'].apply(get_sentiment)

    # ingestion of the processed reviews data to BigQuery
    print("INFO - Sentiment calculation done. Process in BigQuery table")
    dataframe_to_bigquery(
        df=df_reviews,
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
