"""
Este DAG prueba el token de Datapower.

Se debe ejecutar el DAG Init_Inventario para inicializar la interface con Lisy, sólo la primera vez que se vaya a usar la interface.


"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.models import Variable

#import my plugin
from lisy_plugin.operators.lisy_operator import LisyCheckTokenOperator


from datetime import datetime, timedelta
import logging

#arg
default_args = {
    'owner': 'Inventario',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['ProgramaReddeTransporte@teco.com.ar'],
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
    dag_id='lisy_chk_token_DP', 
    schedule_interval= None, 
    description='Lisy API-Rest Interface',
    default_args=default_args
)

#tasks
t0 	= DummyOperator(task_id='dummy_task', retries=1, dag=dag)

t1	= LisyCheckTokenOperator(
    task_id='prueba_token', 
    params={
    },
    dag=dag
)


t0 >> t1 
