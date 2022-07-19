## Airflow

In this project, Airflow was set to run locally.

In [airflow folder](../airflow/) there are the Dockerfile and the docker-compose yaml file:
* [Dockerfile](../airflow/Dockerfile)
* [docker-compose.yaml](airflow/docker-compose.yaml)

The project name and bucket name variables (`GCP_PROJECT_ID` and `GCP_GCS_BUCKET`, respectively) in the [docker-compose.yaml](../airflow/docker-compose.yaml) need to be set to the proper ones, as defined in [setup_gcp.md](setup_gcp.md).

Run Airflow using the following commands:
* `docker-compose build` to build the image;
* `docker-compose up airflow-init` to boot all the services (Airflow scheduler, DB , etc.);
* `docker-compose up` to start all services.

To stop Airflow, run `docker-compose down` command in the terminal.

Run the DAGs:
1. Open [http://localhost:8080/](http://localhost:8080/) in the browser and login using the following account:
* username: `airflow`
* password: `airflow`

2. On the DAGs View page there are 3 DAGs:
* `data_to_gcs_dag` for downloading data from the API and uploading it to GCS in a parquet format, every minute;
* `gcs_to_bq_dag` for creating an external and then optimized table in BigQuery from the data stored in GCS, updated every hour;
* `dataproc_to_bq_dag` to process in Dataproc (Spark) data from GCS and send to BQ for the dashboard, updated every hour.

DAGs are scheduled to run periodically and one just need to trigger them to start in the order described.

The diagrams for each DAG can be seen in the [images folder](../imgs/).