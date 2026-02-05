"""
Airflow DAG for running our data quality pipeline
This handles spinning up clusters, running Spark jobs, and cleaning up after
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# These are the default settings for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,  # Each run is independent
    'email': ['alerts@company.com'],
    'email_on_failure': True,  # Only bug us if something breaks
    'email_on_retry': False,
    'retries': 2,  # Try twice before giving up
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Setting up the DAG
dag = DAG(
    'realtime_data_quality_pipeline',
    default_args=default_args,
    description='Data quality checks running on Spark with GCP/Databricks',
    schedule_interval='@hourly',  # Runs every hour
    catchup=False,  # Don't backfill old runs
    tags=['data-quality', 'spark', 'gcp'],
)

# GCP project settings - change these to match your setup
GCP_PROJECT_ID = 'your-project-id'
GCP_REGION = 'us-central1'
CLUSTER_NAME = 'dq-cluster-{{ ds_nodash }}'
BUCKET_NAME = 'your-bucket-name'

# Cluster specs - this should be enough for most workloads
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512},
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512},
    },
}

# Task 1: Validate the source data exists
def check_source_data(**context):
    # Just a simple check for now, can make this more sophisticated later
    print("Checking if source data is available...")
    # Add your validation logic here
    return True

validate_sources = PythonOperator(
    task_id='validate_sources',
    python_callable=check_source_data,
    dag=dag,
)

# Task 2: Spin up the GCP Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=GCP_REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
)

# Task 3: Run the actual Spark quality checks
run_spark_quality_check = DataprocSubmitPySparkJobOperator(
    task_id='run_spark_quality_check',
    main='gs://{{ params.bucket }}/spark_jobs/data_quality_processor.py',
    cluster_name=CLUSTER_NAME,
    region=GCP_REGION,
    project_id=GCP_PROJECT_ID,
    dataproc_jars=[
        'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
    ],
    arguments=[
        '--input-path', 'gs://{{ params.bucket }}/input/',
        '--output-path', 'gs://{{ params.bucket }}/output/',
        '--config-path', 'gs://{{ params.bucket }}/config/config.yaml',
    ],
    dag=dag,
)

# Task 4: Quick validation to make sure output looks good
def verify_output(**context):
    # Check if the quality metrics were actually generated
    print("Verifying output files exist...")
    # Add your checks here
    return True

validate_output = PythonOperator(
    task_id='validate_output',
    python_callable=verify_output,
    dag=dag,
)

# Task 5: Report the metrics
send_metrics = PythonOperator(
    task_id='send_quality_metrics',
    python_callable=send_quality_metrics,
    dag=dag,
)

# Task 6: Clean up - delete the cluster to save money
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    cluster_name=CLUSTER_NAME,
    trigger_rule='all_done',  # Always delete, even if job failed
    dag=dag,
)

# Task 7: Send success email
send_success_email = EmailOperator(
    task_id='send_success_notification',
    to='team@company.com',
    subject='Data Quality Pipeline Success - {{ ds }}',
    html_content='<p>The data quality pipeline ran successfully!</p>',
    dag=dag,
)

# How the tasks flow - one after another
# This runs with GCP Dataproc
validate_sources >> create_cluster >> run_spark_quality_check >> send_metrics >> delete_cluster >> send_success_email

# If you want to use Databricks instead, use this:
# validate_sources >> run_databricks_job >> send_metrics >> send_success_email
