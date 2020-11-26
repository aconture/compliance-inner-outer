"""
Este DAG ejecuta operadores creados para interactuar con el inventario de Telecom.

Se debe ejecutar el DAG Init_Inventario para inicializar la interface con Lisy, sólo la primera vez que se vaya a usar la interface.
 
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator

from airflow.models import Variable

#import my plugin
#from lisy_plugin.operators.lisy_operator import LisyQueryOperator
#from lisy_plugin.operators.lisy_operator import LisyQueryDNsOperator
from lisy_plugin.operators.lisy_operator import LisyQueryCorporateService
from lisy_plugin.operators.lisy_operator import LisyQueryCustom
from lisy_plugin.operators.lisy_operator import LisyCheckTokenOperator
from lisy_plugin.operators.lisy_operator import LisyQueryPort


from datetime import datetime, timedelta
import os
import pprint
import json
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
    dag_id='lisy_query_dn-via-datapower', 
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

"""
t2	= LisyQueryDNsOperator(
    task_id='query_dn', 
    params={
    },
    metodo = 'cualquiercosa', #El nombre de la query creada en Lisy
     
    # (este objeto da error) objeto = 'root:Locations:ARGENTINA:BUENOS AIRES:CAL:Sala de Rack 1:AH03:ROA1.CAL1:LC0:A9K-40GE-TR 0:10', #==> este es el valor que se usara para filtrar el search en lisy
    #objeto = 'root:Locations:ARGENTINA:CAPITAL FEDERAL:CIO:Sala Discover:A1:CIO2MU:LC1:A9K-48X10GE-1G-SE 1:0',
    objeto = 'root:Locations:ARGENTINA:CAPITAL FEDERAL:CEN:Sala de Rack 1:AE02:ROA1.CEN1:DPC0:DPCE-R-40GE-TX 1:39',
    dag=dag
)
"""

_q_service	= LisyQueryCorporateService(
    #Para ver el servicio, ver documento "API_Obtencion_Recursos_Físicos__v1.1.doc"
    task_id='query_servicio', 
    params={
    },
    #objeto = 'root:Locations:ARGENTINA:BUENOS AIRES:CAL:Sala de Rack 1:AH03:ROA1.CAL1:LC0:A9K-40GE-TR 0:10', #==> este es el valor que se usara para filtrar el search en lisy
    servid = '439341-1', #este serv no tiene asignado IP ni VLAN
    #445535-1, 439302-0, 439341-1 -> otro
    #shelfName: ROA1.CEN1 interfacename: 0/3/7
    dest_dir = '/usr/local/airflow/reports/Lisy/',
    dag=dag
) 

_q_custom	= LisyQueryCustom(#falta que Datapower habilite el endpoint /queries/
    task_id='query_custom', 
    params={
    },
    query_id = 'ARI_HOSTIP',
    dest_dir = '/usr/local/airflow/reports/Lisy/',
    dag=dag
) 


_q_port	= LisyQueryPort(
    task_id='query_port', 
    params={
    },
    shelf_name = 'IC1.HOR1',
    port_id = '9/0/1',
    dest_dir = '/usr/local/airflow/reports/Lisy/',
    dag=dag
) 



t0 >> t1 >> _q_service >> _q_custom
