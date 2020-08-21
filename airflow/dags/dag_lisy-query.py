"""
### Documentacion de la interface Lisy
La informacion de la interface se encuentra en este link
[Sharepoint](https://cablevisionfibertel.sharepoint.com/teams/PlanAutomation/_layouts/15/Doc.aspx?sourcedoc={2033be12-8a48-4a04-ae9f-a2c20dfd54a3}&action=edit&wd=target%28Interfaces.one%7C7ff74e1b-5486-45a3-bd9e-e40dd35c9c42%2FLisy%20%28Draft%5C%29%7C2f942ed9-c6bb-4bc7-8cb4-ae40d44c4558%2F%29&wdorigin=703)
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator

from airflow.models import Variable

#import my plugin
from lisy_plugin.operators.lisy_operator import LisyQueryOperator
from lisy_plugin.operators.lisy_operator import LisyQueryToJsonOperator
from airflow.hooks import PostgresHook

from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import os
import pprint
import json
import logging

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

def load_inv (ds, **kwargs):
    #tiene que estar creada la conexion postgres_conn
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    ##### ===> ME QUEDE ACA para armar el cursor, armar con psycopg2

    #file_name = 'inv_'+str (datetime.now().date())+'.json'
    file_name = 'inv_query.json'
    tot_name = os.path.join('basedatos',file_name)

    logging.info(':::Leyendo: {}'.format(tot_name))
    with open(tot_name,'r') as inputfile:
        inv = json.load(inputfile)
        if type(inv) == list:
            first_record = inv[0]
            columnas = list(first_record.keys())
            logging.info ("\nColumnas encontradas:::{}".format(columnas))
    sql_stringA = 'INSERT INTO inventario ('+ ', '.join(columnas) + ")\nVALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    
    #datos=json.loads(inv)
    #print (datos)
    #print (type(inv))
    #execute_values(cursor, "INSERT INTO inventario VALUES %s", inv)

    print ('Cantidad de registros leidos:::{}',len(inv))
    for i, record_dict in enumerate(inv):
            # itero sobre cada registro devuelto en el query
        values = []
        for col_names, val in record_dict.items():            
            # Postgres strings must be enclosed with single quotes
            if type(val) == str:
                # escape apostrophies with two single quotations
                val = val.replace("'", "''")
                #val = "'" + val + "'"
            values += [ str(val) ]
        # el hook de postgres necesita que el parametro sea una tupla:
        Parametros= tuple(values)
        pg_hook.run(sql_stringA, parameters=Parametros)



#dag
dag = DAG(
    dag_id='dag_lisy_query', 
    schedule_interval= None, 
    description='Lisy API-Rest Interface',
    default_args=default_args
)

#tasks
t0 	= DummyOperator(task_id='dummy_task', retries=1, dag=dag)

t1	= LisyQueryOperator(
    task_id='query_lisy', 
    params={
    },
    metodo = 'Maqueta_sitio_movil', #El nombre de la query creada en Lisy
    objeto = 'UnNumeroDeReferencia1234', #==> este es el valor que se usara para filtrar el search en lisy
    dag=dag
)

t2	= LisyQueryToJsonOperator(
    task_id='extraigo', 
    params={
    },
    metodo = 'Maqueta_sitio_movil', #El nombre de la query creada en Lisy
    objeto = 'UnNumeroDeReferencia1234', #==> este es el valor que se usara para filtrar el search en lisy
    dag=dag
)

t3 = PythonOperator (
    task_id='write_db',
    provide_context = True,
    python_callable = load_inv,
    dag = dag
)


t0 >> t1 >> t2 >> t3
