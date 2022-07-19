import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import  DataprocSubmitPySparkJobOperator, DataprocDeleteClusterOperator, DataprocCreateClusterOperator
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
REGION =  "europe-west2"
DATAPROC_CLUSTER = "cpereira-cluster"
SPARKPATH = "/opt/airflow/dataproc"
source_file_name = '/process_hourly_coin_price.py'
source_path_file = SPARKPATH + source_file_name
gcs_path_file = f'sources{source_file_name}'
gcs_source_uri = f'gs://{BUCKET}/{gcs_path_file}'


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

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

# Cluster config, minimum setup below quota
CLUSTER_CONFIG = {
   "master_config": {
       "num_instances": 1,
       "machine_type_uri": "n1-standard-2",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 256},
   },
   "worker_config": {
       "num_instances": 2,
       "machine_type_uri": "n1-standard-2",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 256},
   },
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="dataproc_to_bq_dag",
    schedule_interval="10 * * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['coin-price'],
) as dag:

    # as defined in FAQ, need CreateCluster, SubmitJob, and DestroyCluster; jar needs to be added
    create_dataproc_cluster_task=DataprocCreateClusterOperator(
        task_id = 'create_dataproc_cluster_task',
        project_id = PROJECT_ID,
        cluster_name = DATAPROC_CLUSTER,
        cluster_config=CLUSTER_CONFIG,
        region = REGION
    )

    upload_main_to_gcs_task = PythonOperator(
        task_id=f"upload_main_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": gcs_path_file,
            "local_file": source_path_file,
        },
    )

    submit_dataproc_spark_job_task = DataprocSubmitPySparkJobOperator(
        task_id = "submit_dataproc_spark_job_task",
        main = gcs_source_uri,
        arguments = ['{{ execution_date.subtract(hours=1).strftime(\'%Y-%m-%d %H:%M\') }}'],
        region = REGION,
        cluster_name = DATAPROC_CLUSTER,
        dataproc_jars = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar"]
    )

    delete_dataproc_cluster_task = DataprocDeleteClusterOperator(
        task_id = "delete_dataproc_cluster_task",
        project_id = PROJECT_ID,
        cluster_name = DATAPROC_CLUSTER,
        region = REGION,
        trigger_rule = "all_done", #all_done means all operations have finished working. Maybe they succeeded, maybe not.
    )

    create_dataproc_cluster_task >> upload_main_to_gcs_task >> submit_dataproc_spark_job_task >> delete_dataproc_cluster_task