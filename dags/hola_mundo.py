from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from time import sleep
from datetime import datetime

def print_hello():
	sleep(5)
	return 'Hola Mundo!!!'

with DAG('hola_mundo_dag', description='DAG ejemplo', schedule_interval='*/10 * * * *', start_date=datetime(2020, 10, 19), catchup=False) as dag:
	dummy_task 	= DummyOperator(task_id='dummy_task', retries=3)
	python_task	= PythonOperator(task_id='python_task', python_callable=print_hello)

	dummy_task >> python_task
