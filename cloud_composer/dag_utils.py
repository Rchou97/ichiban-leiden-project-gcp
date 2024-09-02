import google.auth.transport.requests

from airflow.providers.google.common.utils import id_token_credentials
from google.auth.transport.requests import AuthorizedSession

def invoke_cloud_function(cloud_function_url: str):
    # The url is also the target audience
    url = cloud_function_url

    # This is a request for obtaining the credentials
    request = google.auth.transport.requests.Request()

    # If your cloud function url has query parameters, remove them before passing to the audience
    id_token = id_token_credentials.get_default_id_token_credentials(url, request=request)

    # The authorized session object is used to access the Cloud Function
    resp = AuthorizedSession(id_token).request("GET", url=url)

    # Should return 200
    print(resp.status_code)

    # The body of the HTTP response
    print(resp.content)
