from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from time import sleep
from datetime import datetime, timedelta

#arg
default_args = {
    'owner': 'Adrian Contureyuzon',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['aconture@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'provide_context': True,
    'dag_type': 'custom'
}

#dag
dag = DAG(
    dag_id='Hola_Mundo_2', 
    schedule_interval= None, 
    default_args=default_args
)

def print_hello(**kwargs):
    sleep(5)
    return ('Hola Telecom! desde Hola_Mundo_2')

#tasks
t0 = DummyOperator(task_id='dummy_task', retries=1, dag=dag)

t1 = PythonOperator(
    task_id='ejecuto_python',
    python_callable=print_hello,
    dag=dag
)

t0 >> t1