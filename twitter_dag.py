from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 14, 10, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Dependant_Dag_Ahmad',
    default_args=default_args,
    description='Our first DAG with ETL process 4 tasks!',
    schedule_interval=timedelta(minutes=5),
)

# Make two - three processes dependant on each other aha
#

# run_etl = PythonOperator(
#     task_id='complete_twitter_etl',
#     python_callable=run_twitter_etl,
#     dag=dag, 
# )

task1_etl = PythonOperator(
    task_id='Task1',
    python_callable=run_twitter_etl_1,
    dag=dag, 
)

task2_etl = PythonOperator(
    task_id='Task2',
    python_callable=run_twitter_etl_2,
    dag=dag, 
)

task3_etl = PythonOperator(
    task_id='Task3',
    python_callable=run_twitter_etl_3,
    dag=dag, 
)

task4_etl = PythonOperator(
    task_id='Task4',
    python_callable=run_twitter_etl_4,
    dag=dag, 
)


task1_etl >> [task2_etl, task3_etl]

[task2_etl, task3_etl] >> task4_etl