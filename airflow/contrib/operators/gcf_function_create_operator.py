import requests
from time import sleep

from airflow.contrib.hooks.gcf_hook import GCFHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GCFFunctionCreateOperator(BaseOperator):
    template_fields = ['cluster_name', 'project_id', 'zone', 'region']

    @apply_defaults
    def __init__(self,
                 function_name,
                 project_id,
                 region,
                 runtime='node6',
                 function_zip_local_path=None,
                 function_zip_gcs_path=None,
                 *args, **kwargs):
        self.function_name = function_name
        self.project_id = project_id
        self.region = region
        self.runtime = runtime
        self.function_zip_local_path = function_zip_local_path
        self.function_zip_gcs_path = function_zip_gcs_path
        super(GCFFunctionCreateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = GCFHook()
        service = hook.get_conn()
        location = 'projects/{}/locations/{}'.format(self.project_id, self.region)
        function_name = '{}/functions/{}'.format(location, self.function_name)

        for _function in hook.list_functions(location):
            if _function["name"] == function_name:
                print("Function already exists")
                return

        function_create_body = {
            'name': function_name,
            'runtime': self.runtime,
            'httpsTrigger': {}
        }
        if self.function_zip_local_path:
            rsp = service.projects().locations().functions().generateUploadUrl(
                parent='projects/{}/locations/{}'.format(self.project_id, self.region)
            ).execute()  # TODO: move to hook
            uploadURL = rsp.get('uploadUrl')

            with open(self.function_zip_local_path, 'rb') as fp:
                requests.put(
                    uploadURL,
                    data=fp.read(),
                    headers={
                        'Content-type': 'application/zip',
                        'x-goog-content-length-range': '0,104857600',
                    }
                )
            function_create_body['sourceUploadUrl'] = uploadURL
        elif self.function_zip_gcs_path:
            function_create_body['sourceArchiveUrl'] = self.function_zip_gcs_path
        else:
            # TODO sourceRepository
            raise AttributeError("Missing source")

        service.projects().locations().functions().create(
            location=location,
            body=function_create_body
        ).execute()  # TODO: move to hook

        while True:
            f = service.projects().locations().functions().get(name=function_name).execute()  # TODO: move to hook
            status = f.get('status')
            print(status)
            if status == 'ACTIVE':
                break
            sleep(1)

        return 'DONE'
