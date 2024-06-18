# dags/aws_example.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3

def list_s3_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(f'Bucket: {bucket["Name"]}')
        

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG('aws_example_3',
         default_args=default_args,
         schedule_interval=None,
         ) as dag:

    list_buckets = PythonOperator(
        task_id='list_s3_buckets',
        python_callable=list_s3_buckets,
    )

    list_buckets
