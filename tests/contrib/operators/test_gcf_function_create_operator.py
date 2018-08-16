import unittest

from airflow.contrib.operators.gcf_function_create_operator import GCFFunctionCreateOperator


class GCfFunctionCreateTest(unittest.TestCase):
    def test_create_function_with_gcs_path(self):
        op = GCFFunctionCreateOperator(
            function_name="helloWorld",
            project_id="polidea-airflow",
            region="europe-west1",
            function_zip_gcs_path="gs://polidea-functions/hello.zip",
            task_id="id"
        )
        op.execute(None)
