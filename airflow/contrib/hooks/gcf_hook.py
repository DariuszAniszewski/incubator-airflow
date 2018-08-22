from time import sleep

import requests
from googleapiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class GCFHook(GoogleCloudBaseHook):
    """Hook for Google Cloud Functions APIs."""
    MAX_RETRIES = 1000

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

    def create_new_function(self, location, body):
        response = self.get_conn().projects().locations().functions().create(
            location=location,
            body=body
        ).execute()
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def update_function(self, name, body, fields):
        response = self.get_conn().projects().locations().functions().patch(
            updateMask=",".join(fields),
            name=name,
            body=body
        ).execute()
        operation_name = response["name"]
        return self._wait_for_operation_to_complete(operation_name)

    def upload_function_zip(self, parent, zip_path):
        response = self.get_conn().projects().locations().functions().generateUploadUrl(
            parent=parent
        ).execute()
        uploadURL = response.get('uploadUrl')
        with open(zip_path, 'rb') as fp:
            requests.put(
                uploadURL,
                data=fp.read(),
                headers={
                    'Content-type': 'application/zip',
                    'x-goog-content-length-range': '0,104857600',
                }
            )
        return uploadURL

    def _wait_for_operation_to_complete(self, operation_name):
        service = self.get_conn()
        retries = 0
        while True:
            if retries > self.MAX_RETRIES:
                raise Exception("Too many retries")

            operation_response = service.operations().get(
                name=operation_name,
            ).execute()

            if operation_response.get("done"):
                return operation_response["response"]

            retries += 1
            sleep(5)
