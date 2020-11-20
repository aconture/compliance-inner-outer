from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from time import sleep
from datetime import datetime, timedelta

#arg
default_args = {
    'owner': 'Telecom in house Automation',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['automation@teco.com.ar'],
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
    dag_id='Inicaliza_Postgres', 
    schedule_interval= None, 
    default_args=default_args
)

#tasks
t0 = DummyOperator(task_id='inicio', retries=1, dag=dag)

t1 = PostgresOperator(
    task_id='Crea_estructura_db',
    postgres_conn_id='postgres_conn',
    sql=open('/usr/local/airflow/script/script_bd.sql').read(),
    dag=dag
 )

t0 >> t1




 


   
    