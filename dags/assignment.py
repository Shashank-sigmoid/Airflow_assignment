from task1 import task_1
from task2 import task_2
from task3 import task_3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Shashank",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 16),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Scheduling the DAG in airflow to run every day at 6:00 AM
dag = DAG("assignment", default_args=default_args, schedule_interval="0 6 * * *")

t1 = PythonOperator(task_id="Write_CSV_file", python_callable=task_1, dag=dag)

t2 = PythonOperator(task_id="Create_table_weather", python_callable=task_2, dag=dag)

t3 = PythonOperator(task_id="Insert_data", python_callable=task_3, dag=dag)

t1 >> t2 >> t3
