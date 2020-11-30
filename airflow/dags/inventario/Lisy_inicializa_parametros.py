"""
Este DAG crea la estructura que necesita para correr el plugin de Inventario.
Se ejecuta una sola vez, para inicializar los objetos necesarios.


DAG par probar la interface con Lisy.

CONN:

'Conn Id': lisy_token_test 
'Conn Type': HTTP 
'Host':  sesiont.personal.com.ar
'Login':  LisyFullConsumer
'Password': password de credencial correspondiente
'Port': 

'Conn Id': lisy_api 
'Conn Type': HTTP 
'Host': apit.telecom.com.ar
'Login':  -
'Password': -
'Port': 

"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

#arg
default_args = {
    'owner': 'Inventario',
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
    dag_id='Init_Inventario', 
    schedule_interval= None, 
    default_args=default_args
)

#tasks

#crea las conexiones necesarias para el funcionamiento de la interface con inventario
_init_conn = BashOperator(
    task_id='Init_conn',
    bash_command='airflow connections -a --conn_id Lisy_api --conn_type http --conn_host apit.telecom.com.ar --conn_port 443; airflow connections -a --conn_id Lisy_token_test --conn_type http --conn_host sesiont.personal.com.ar --conn_login LisyFullConsumer --conn_port 443 --conn_password Teco2020_',
    dag=dag
 )
 
#crea los directorios necesarios para el funcionamiento de la interface con inventario
_crea_dir = BashOperator(
    task_id='Crea_dir',
    bash_command='mkdir /usr/local/airflow/reports/Lisy/',
    dag=dag
 ) 

_init_conn >> _crea_dir




 


   
    