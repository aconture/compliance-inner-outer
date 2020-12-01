"""Este DAG se utiliza para crear las conexiones necesarias entre Airflow y los distintos elementos del Framework.
La contraseña para las credenciales de Ansible debe cargarse manualmente."""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from time import sleep
from datetime import datetime, timedelta

#args
default_args={
    'owner': 'Plataforma',
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

#DAG
dag = DAG(
    dag_id='Init_Airflow_Connections', 
    schedule_interval= None, 
    default_args=default_args
)


#Tasks

#inicio
t0 = DummyOperator(task_id='inicio', retries=1, dag=dag)

#creo conexion con Ansible para las credenciales de los equipos
t1 = BashOperator(
    task_id='Crea_conexion_Ansible',
    bash_command='airflow connections --add --conn_id credenciales_equipos --conn_login x304372 --conn_type http',
    dag=dag
    )

#creo conexion con postgres
t2 = BashOperator(
    task_id='Crea_conexion_Postgres',
    bash_command='airflow connections -a --conn_id postgres_conn --conn_type postgres --conn_host airflow-postgres --conn_schema airflow --conn_login airflow --conn_port 5432 --conn_password airflow',
    dag=dag
    )

#creo conexion con Influx
t3 = BashOperator(
    task_id='Crea_conexion_Influx',
    bash_command='airflow connections --add --conn_id influxdb_conn --conn_login admin --conn_password Welcome1 --conn_port 8086 --conn_schema influx_airflow --conn_host influxdb --conn_type http',
    dag=dag
    )

#creo conexion con MongoDB

t4 = BashOperator(
    task_id='Crea_conexion_MongoDB',
    bash_command='airflow connections --add --conn_id mongodb_conn --conn_login admin --conn_port 27017 --conn_schema temporal --conn_host airflow-mongodb --conn_type mongo',
    dag=dag
    )

t0 >> t1 >> t2 >> t3 >> t4