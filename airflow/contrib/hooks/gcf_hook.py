from googleapiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class GCFHook(GoogleCloudBaseHook):
    """Hook for Google Cloud Functions APIs."""

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 api_version='v1'):
        super(GCFHook, self).__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self):
        """Returns a Google Cloud Functions service object."""
        http_authorized = self._authorize()
        return build(
            'cloudfunctions', self.api_version, http=http_authorized,
            cache_discovery=False)

    def list_functions(self, location):
        list_response = self.get_conn().projects().locations().functions().list(parent=location).execute()
        return list_response.get("functions", [])
