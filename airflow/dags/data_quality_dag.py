"""
Airflow DAG for orchestrating real-time data quality pipeline
Runs Spark jobs on GCP and Databricks for data quality monitoring
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

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'realtime_data_quality_pipeline',
    default_args=default_args,
    description='End-to-end data quality monitoring with Spark on GCP/Databricks',
    schedule_interval='@hourly',
    catchup=False,
    tags=['data-quality', 'spark', 'realtime'],
)

# GCP Configuration
GCP_PROJECT_ID = 'your-gcp-project-id'
GCP_REGION = 'us-central1'
CLUSTER_NAME = 'data-quality-cluster'
GCS_BUCKET = 'your-gcs-bucket'

# Databricks Configuration
DATABRICKS_CONN_ID = 'databricks_default'
NOTEBOOK_PATH = '/Workspace/data_quality/processor'

def validate_data_sources(**context):
    """Validate that source data is available before processing"""
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Validating data sources...")
    # Add validation logic here
    return True

def send_quality_metrics(**context):
    """Send quality metrics to monitoring system"""
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Sending quality metrics to monitoring dashboard...")
    # Add metrics reporting logic
    return True

# Task 1: Validate data sources
validate_sources = PythonOperator(
    task_id='validate_data_sources',
    python_callable=validate_data_sources,
    dag=dag,
)

# Task 2: Create GCP Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    cluster_name=CLUSTER_NAME,
    cluster_config={
        'master_config': {
            'num_instances': 1,
            'machine_type_uri': 'n1-standard-4',
            'disk_config': {
                'boot_disk_size_gb': 500,
            }
        },
        'worker_config': {
            'num_instances': 2,
            'machine_type_uri': 'n1-standard-4',
            'disk_config': {
                'boot_disk_size_gb': 500,
            }
        },
    },
    dag=dag,
)

# Task 3: Submit Spark job to GCP Dataproc
run_spark_quality_check = DataprocSubmitPySparkJobOperator(
    task_id='run_spark_data_quality',
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    cluster_name=CLUSTER_NAME,
    main=f'gs://{GCS_BUCKET}/spark_jobs/data_quality_processor.py',
    arguments=[
        f'--input=gs://{GCS_BUCKET}/raw-data/',
        f'--output=gs://{GCS_BUCKET}/quality-checked-data/',
        f'--checkpoint=gs://{GCS_BUCKET}/checkpoints/',
    ],
    dag=dag,
)

# Task 4: Run Databricks notebook (alternative to GCP)
run_databricks_job = DatabricksSubmitRunOperator(
    task_id='run_databricks_quality_check',
    databricks_conn_id=DATABRICKS_CONN_ID,
    notebook_task={
        'notebook_path': NOTEBOOK_PATH,
        'base_parameters': {
            'input_path': 'dbfs:/raw-data/',
            'output_path': 'dbfs:/quality-checked-data/',
        }
    },
    new_cluster={
        'spark_version': '11.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 2,
        'spark_conf': {
            'spark.speculation': True,
        },
    },
    dag=dag,
)

# Task 5: Send quality metrics
send_metrics = PythonOperator(
    task_id='send_quality_metrics',
    python_callable=send_quality_metrics,
    dag=dag,
)

# Task 6: Delete GCP cluster
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    cluster_name=CLUSTER_NAME,
    trigger_rule='all_done',  # Run even if upstream fails
    dag=dag,
)

# Task 7: Send success notification
send_success_email = EmailOperator(
    task_id='send_success_notification',
    to='team@company.com',
    subject='Data Quality Pipeline Success - {{ ds }}',
    html_content='<p>Data quality pipeline completed successfully.</p>',
    dag=dag,
)

# Define task dependencies
validate_sources >> create_cluster >> run_spark_quality_check >> send_metrics >> delete_cluster >> send_success_email
# Alternative path using Databricks (comment out GCP path if using this)
# validate_sources >> run_databricks_job >> send_metrics >> send_success_email
