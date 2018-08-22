from airflow.models import BaseOperator
from airflow.contrib.hooks.gcf_hook import GCFHook
from airflow.utils.decorators import apply_defaults


class GCFFunctionCreateOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 function_name,
                 project_id,
                 region,
                 entrypoint,
                 runtime='nodejs6',
                 function_zip_local_path=None,
                 function_zip_gcs_path=None,
                 *args, **kwargs):
        super(GCFFunctionCreateOperator, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.entrypoint = entrypoint
        self.runtime = runtime
        self.function_zip_local_path = function_zip_local_path
        self.function_zip_gcs_path = function_zip_gcs_path
        self.location = 'projects/{}/locations/{}'.format(self.project_id, self.region)
        self.function_name = '{}/functions/{}'.format(self.location, function_name)

    def _prepare_function_body(self, hook):
        body = {
            'name': self.function_name,
            'runtime': self.runtime,
            'entryPoint': self.entrypoint,
            'httpsTrigger': {}
        }
        if self.function_zip_local_path:
            upload_url = hook.upload_function_zip(self.location, self.function_zip_local_path)
            body['sourceUploadUrl'] = upload_url
        elif self.function_zip_gcs_path:
            body['sourceArchiveUrl'] = self.function_zip_gcs_path
        else:
            # TODO sourceRepository
            raise AttributeError("Missing source")
        return body

    def _create_new_function(self, hook):
        body = self._prepare_function_body(hook)
        hook.create_new_function(self.location, body)

    def _update_function(self, hook):
        body = self._prepare_function_body(hook)
        hook.update_function(self.function_name, body, body.keys())

    def _check_if_function_exists(self, hook):
        return self.function_name in map(
            lambda x: x["name"], hook.list_functions(self.location)
        )

    def execute(self, context):
        hook = GCFHook()

        if not self._check_if_function_exists(hook):
            self._create_new_function(hook)
        else:
            self._update_function(hook)
