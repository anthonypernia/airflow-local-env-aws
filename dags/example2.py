# dags/aws_s3_example.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import logging

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'aws_s3_example',
    default_args=default_args,
    description='A simple tutorial DAG to interact with AWS S3',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

# Define the Python callable function
def list_s3_buckets():
    # Create a session using the credentials from the environment
    session = boto3.Session()
    # Create an S3 client
    s3 = session.client('s3')
    # List the S3 buckets
    response = s3.list_buckets()
    # Log the bucket names
    for bucket in response['Buckets']:
        logging.info(f'Bucket: {bucket["Name"]}')
        print(f'Bucket: {bucket["Name"]}')

# Define the PythonOperator
list_buckets_task = PythonOperator(
    task_id='list_s3_buckets',
    python_callable=list_s3_buckets,
    dag=dag,
)

# Set the task dependencies
list_buckets_task
