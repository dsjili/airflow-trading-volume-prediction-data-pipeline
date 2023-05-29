import sys
import os

# Get the absolute path of the current file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Append the scripts folder path to sys.path
scripts_folder_path = os.path.join(current_dir, '..', 'scripts')
sys.path.append(scripts_folder_path)

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from preprocess import preprocess
from features_transform import features_transform
from train import train
from deploy import deploy

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 28),
    'retries': 0,
    'pool': 'default_pool'
}

dag = DAG(
    dag_id='trading_volume_prediction',
    default_args=default_args,
)

preprocess_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess,
    op_kwargs={
        'input_path': 'data/',
        'output_path': 'data/'
    },
    dag=dag
)

features_engineering_task = PythonOperator(
    task_id='features_engineering',
    python_callable=features_transform,
    op_kwargs={
        'input_path': 'data/etfs_stocks.parquet',
        'output_path': 'data/stage/'
    },
    dag=dag
)

training_task = PythonOperator(
    task_id='training',
    python_callable=train,
    op_kwargs={
        'input_path': 'data/stage/etfs_stocks.parquet',
        'output_path': 'data/deploy/'
    },
    dag=dag
)

deploy_task = PythonOperator(
    task_id='deploy',
    python_callable=deploy,
    op_kwargs={
        'input_path': 'data/deploy/model.joblib',
        'bucket_name': 'dsjili-github',
        'output_key': 'trading-volume-precition/model.joblib'
        
    },
    dag=dag
)

preprocess_task >> features_engineering_task >> training_task >> deploy_task