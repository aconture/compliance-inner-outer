"""
### Documentacion de la interface Lisy
La informacion de la interface se encuentra en este link
[Sharepoint](https://cablevisionfibertel.sharepoint.com/teams/PlanAutomation/_layouts/15/Doc.aspx?sourcedoc={2033be12-8a48-4a04-ae9f-a2c20dfd54a3}&action=edit&wd=target%28Interfaces.one%7C7ff74e1b-5486-45a3-bd9e-e40dd35c9c42%2FLisy%20%28Draft%5C%29%7C2f942ed9-c6bb-4bc7-8cb4-ae40d44c4558%2F%29&wdorigin=703)
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from airflow.models import Variable

#import my plugin
from lisy_plugin.operators.lisy_operator import LisyQueryOperator
from lisy_plugin.operators.lisy_operator import LisyHelpOperator
from lisy_plugin.operators.lisy_operator import LisyAboutOperator
from lisy_plugin.operators.lisy_operator import LisyCheckOperator

from datetime import datetime, timedelta

#arg
default_args = {
    'owner': 'Team Transformacion',
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
    dag_id='dag_lisy_test', 
    schedule_interval= None, 
    description='Lisy API-Rest Interface',
    default_args=default_args
)

#tasks
t0 	= DummyOperator(task_id='dummy_task', retries=1, dag=dag)

#Instancio la Documentacion del DAG
dag.doc_md = __doc__

t1	= LisyQueryOperator(
    task_id='query_lisy', 
    params={
    },
    metodo = 'AllShelf_and_clasification_demo', #El nombre de la query creada en Lisy
    objeto = 'UnNumeroDeReferencia1234', #==> este es el valor que se usara para filtrar el search en lisy
    dag=dag
)

t2	= LisyHelpOperator(
    task_id='ayuda_lisy', 
    params={
    },
    metodo = 'help',
    objeto = 'Referencia4343', 
    solotoken = 'no',
    dag=dag
)

t3	= LisyAboutOperator(
    task_id='about_lisy', 
    params={
    },
    metodo = 'fruta',
    objeto = 'Referencia4343', 
    solotoken = 'no',
    dag=dag
)

t4	= LisyCheckOperator(
    task_id='chk_token', 
    params={
    },
    dag=dag
)


t0 >> t3 >> t1 >> t2
