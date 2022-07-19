import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET_CURRENCY = "EUR"
OUTPUT_PART = "raw"
dataset_file = '{{ execution_date.strftime(\'%Y-%m-%d-%H-%M\') }}-coin-price.parquet'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

with DAG(
    dag_id="data_to_gcs_dag",
    schedule_interval="*/1 * * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['coin-price'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"python /opt/airflow/dags/extract_minute_data.py --currency={DATASET_CURRENCY} --output={path_to_local_home}/{dataset_file}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{OUTPUT_PART}/{{{{ execution_date.strftime(\'%Y-%m-%d\') }}}}/{{{{  execution_date.strftime(\'%H\') }}}}/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        }
    )

    clean_dataset_task = BashOperator(
        task_id="clean_dataset_task",
        bash_command=f"rm {path_to_local_home}/{dataset_file}"
    )

    download_dataset_task >> local_to_gcs_task >> clean_dataset_task