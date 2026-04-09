from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import clickhouse_connect
import os

# 1. Connection Logic
def load_csv_to_clickhouse(table_name):
    # This host 'crm_clickhouse' matches your service name in docker-compose
    client = clickhouse_connect.get_client(
        host=os.getenv('CH_HOST', ''), 
        port=int(os.getenv('CH_PORT', 0)), 
        username=os.getenv('CH_USER', ''), 
        password=os.getenv('CH_PASSWORD', '')
    )
    
    file_path = f'/opt/airflow/data/{table_name}.csv'
    
    if os.path.exists(file_path):
        print(f"Loading {file_path} into crm_raw.{table_name}")
        df = pd.read_csv(file_path)
        
        # We cast everything to string for the RAW landing layer
        df = df.astype(str)
        
        # Insert into ClickHouse
        client.insert_df(
            database='crm_raw', 
            table=table_name, 
            df=df
        )
    else:
        raise FileNotFoundError(f"File {file_path} not found!")

# 2. DAG Definition
default_args = {
    'owner': 'zakar',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crm_ingestion',
    default_args=default_args,
    description='Daily ingestion of CRM CSVs into ClickHouse',
    schedule_interval='@daily', # This triggers every day at midnight (00:00)
    catchup=False
) as dag:

    # List of tables based on your CSV files
    tables = [
        'activity', 
        'activity_types', 
        'deal_changes', 
        'fields', 
        'stages', 
        'users'
    ]

    # 1. Ingestion Phase
    # Initialize the list first!
    ingestion_tasks = []

    for table in tables:
        t = PythonOperator(
            task_id=f'load_{table}_to_raw',
            python_callable=load_csv_to_clickhouse,
            op_kwargs={'table_name': table},
        )
        # Add each task to the list
        ingestion_tasks.append(t)

    # 2. Transformation Phase (dbt)
    dbt_transform = BashOperator(
        task_id='dbt_transform_marts',
        bash_command='cd /opt/airflow/dbt_crm && dbt build',
    )

    # 3. Setting Dependencies
    # Now that the list is populated, this will work
    ingestion_tasks >> dbt_transform