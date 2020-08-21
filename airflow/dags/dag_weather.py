from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from airflow.models import Variable

#import my plugin
from clima_plugin.operators.clima_operator import DisplayClimaOperator

from datetime import datetime, timedelta

LOCATION="Buenos Aires"
my_api_key=Variable.get("MY_API_KEY_CLIMA")

#arg
default_args = {
    'owner': 'Oscar Paniagua',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['opaniagu@gmail.com'],
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
    dag_id='dag_weather', 
    schedule_interval= None, 
    default_args=default_args
)

#tasks
t0 	= DummyOperator(task_id='dummy_task', retries=1, dag=dag)

t1	= DisplayClimaOperator(
    task_id='clima', 
    params={
        "location": LOCATION
    },
    location="Lima", #aca hardcodea la locacion
    api_key=my_api_key,
    dag=dag
)

t0 >> t1

