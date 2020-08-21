from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks import PostgresHook

from time import sleep
from datetime import datetime, timedelta
import os
import logging


#arg
default_args = {
    'owner': 'TT',
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
    dag_id='Compliance_Inner', 
    schedule_interval= None,
    tags=['inventario', 'compliance'],
    default_args=default_args
)

#####################################################################3


def Load_inv(**context):
    manual = """
    Esta funcion carga los registros leidos del archivo indicado en la base de postgres
    
    Args: 
      dir [text]: directorio dentro del home de Airflow, donde se encuentra el archivo o el grupo de archivos a cargar. No debe incluir '/' al final.
      table [text]: tabla de la base de datos que se debe cargar.
      
      Opcional:
      file [text]: archivo con la base a cargar. Si no existe este parametro, se cargan todos los archivos del directorio indicado.
    
    Returns:
      none
    """
    from psycopg2.extras import execute_values
    import pandas as pd

    file=context['file']
    dir=context['dir']
    table=context['table']

    """
    file=context['params'].get('file')
    dir=context['params'].get('dir')
    table=context['params'].get('table')
    """

    if (dir is None) or (table is None):
        logging.info ('\n:::! Error - Falta un argumento de llamada a esta funcion.')
        logging.info (manual)
        return -1

    if file is None:
        #cargo en una lista todos los archivos del directorio
        archivos=os.listdir(os.path.join(os.getcwd(),dir))
    else:
        #lista con el archivo que vino como argumento
        archivos = [file]
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()

    #init de la base
    sql_delete = 'DELETE FROM {}'.format(table)
    pg_cursor.execute(sql_delete)
    logging.info('\n::: Tabla \'{}\' inicializada.'.format(table))

    #var para log:
    file_ok = []
    len_ok = []
    file_nok = []
    len_nok = []
    
    # implementa BD:
    for nom_archivo in archivos:
        abspath = os.path.join(os.getcwd(),dir,nom_archivo)
        try:
            df = pd.read_csv(abspath,delimiter='|')
            #logging.info ('\n::: Cargando desde Archivo{0}, {1} registros.'.format(nom_archivo,len(df)))
            columnas = df.columns.ravel()
            sql_string = 'INSERT INTO {} ('.format(table)+ ', '.join(columnas) + ") (VALUES %s)"

            #logging.info('\n::: Preparando SQL con columnas detectadas: {}'.format(sql_string))
            #El cursor execute_values necesita un array de tuplas, que lo obtengo de esta manera:
            values = list(df.itertuples(index=False, name=None))

            execute_values(pg_cursor, sql_string, values)
            file_ok.append(abspath)
            len_ok.append(len(values))
            #logging.info ('\n::: Populada tabla \'{}\' con {} registros, tomados de {}.'.format(table,len(values),abspath))
        except FileNotFoundError as e:
            logging.info('\n\n:::! Error - No se encuentra el archivo origen {}\n'.format(nom_archivo))
            logging.info('\n{}'.format(manual))
            return
        except Exception as e:
            logging.error ('\n\n:::! Error leyendo registros del archivo {}\n'.format(abspath),exc_info=True)
            file_nok.append(abspath)
            len_nok.append(len(values))

    conn.commit()
    conn.close()
    
    print ('\n--------------------------------')
    print ('::: Resumen de archivos con errores:')
    for i in range(len(file_nok)):
        print ('::: {}: {}'.format(file_nok[i],len_nok[i]))
    
    print ('\n--------------------------------')
    print ('::: Resumen de archivos implementados exitosamente en la tabla \'{}\':'.format(table))
    for i in range(len(file_ok)):
        print ('::: {}: {}'.format(file_ok[i],len_ok[i]))
    print ('\n--------------------------------')

def naming_inv(**context):
    manual = """
    Esta funcion modifica el contenido de ciertos campos traidos desde el inventario para que puedan ser comparados con el archivo que trae ansible desde el NE.
    La lectura la realiza de la tabla inv_itf.
    El resultado lo guarda en la tabla par_inv_itf.

    Args: 
      none 
    Returns:
      none
    """
    from psycopg2.extras import execute_values
    import pandas as pd

    table_ = 'inv_itf'
    table_dest = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()    

    df_inv_itf = pd.read_sql_query('select * from {}'.format(table_),con=conn)

    #filtro1:
    
    #adecuaciones especificas para el Inner Core
    for indice in df_inv_itf.index:
        if (df_inv_itf.loc[indice,'shelfname'].startswith('IC1.')) and (df_inv_itf.loc[indice,'bandwidth'] == '10 Gb'):
            df_inv_itf.loc[indice,'userlabel'] = df_inv_itf.loc[indice,'userlabel']+'(100M)'
            #print (df_inv_itf.loc[indice,'userlabel'])


    #Adecuaciones masivas
    df_inv_itf['bandwidth'] = df_inv_itf['bandwidth'].str.replace('100 Gb','100GE')
    df_inv_itf['bandwidth'] = df_inv_itf['bandwidth'].str.replace('10 Gb','GE')
    df_inv_itf['concat'] = df_inv_itf[['shelfname','bandwidth','userlabel']].agg(''.join, axis=1)


    #init de la base destino
    sql_delete = 'DELETE FROM {}'.format(table_dest)
    pg_cursor.execute(sql_delete)
    logging.info('\n::: Tabla {} inicializada.'.format(table_dest))

    #populo la base destino
    columnas = df_inv_itf.columns.ravel()
    sql_string = 'INSERT INTO {} ('.format(table_dest)+ ', '.join(columnas) + ")\n(VALUES %s)"

    logging.info('\n::: Preparando para ejecutar sql con las columnas detectadas {}'.format(sql_string))
    values = list(df_inv_itf.itertuples(index=False, name=None))
    execute_values(pg_cursor, sql_string, values)

    logging.info ('\n::: Se popula la tabla {} con {} registros.'.format(table_dest,len(values)))

    conn.commit()
    conn.close()

def naming_ne(**context):
    manual = """
    Esta funcion modifica el contenido de ciertos campos traidos desde los NE.
    La lectura la realiza de la tabla NE.
    El resultado lo guarda en la tabla NE.

    Args: 
      none
    Returns:
      none
    """
    from psycopg2.extras import execute_values
    import pandas as pd

    table_ = 'NE'
    table_dest = 'NE'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()    

    df_ne = pd.read_sql_query('select * from {}'.format(table_),con=conn)

    #Adecuaciones
    df_ne['concat'] = df_ne[['shelfname','interface']].agg(''.join, axis=1)

    #init de la base destino
    sql_delete = 'DELETE FROM {}'.format(table_dest)
    pg_cursor.execute(sql_delete)
    logging.info('\n::: Tabla {} inicializada.'.format(table_dest))

    #populo la base destino
    columnas = df_ne.columns.ravel()
    sql_string = 'INSERT INTO {} ('.format(table_dest)+ ', '.join(columnas) + ")\n(VALUES %s)"

    logging.info('\n::: Preparando para ejecutar sql con las columnas detectadas {}'.format(sql_string))
    values = list(df_ne.itertuples(index=False, name=None))
    execute_values(pg_cursor, sql_string, values)

    logging.info ('\n::: Se popula la tabla {} con {} registros.'.format(table_dest,len(values)))

    conn.commit()
    conn.close()

def Caso1_ok(**context):
    manual = """
            Esta funcion determina los registros correctamente sincronizados entre el NE y el inventario.
    Args: 
      none
    Returns:
      none
    """
    import pandas as pd

    table_A = 'ne'
    table_B = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    #casos ok:
    #estan en inventario y en NE, caso ok 1:
    df_ok1 = pd.read_sql_query("""select * from {0} WHERE concat IN
                                (
                                select concat from {0} a where portoperationalstate = 'up'
                                INTERSECT
                                select concat from {1} b where portoperationalstate IN ('Active')
                                )"""
                                .format(table_A,table_B),con=conn)

    #estan en inventario y en NE, caso ok 2:
    df_ok2 = pd.read_sql_query("""select * from {0} WHERE concat IN 
                                (
                                select concat from {0} a where portoperationalstate = 'down'
                                INTERSECT
                                select concat from {1} b where portoperationalstate NOT IN ('Active')
                                )"""
                                .format(table_A,table_B),con=conn)

    df_ok = pd.concat ([df_ok1,df_ok2])



    conn.close()


    #impresiones:
    print (len(df_ok))

    df_ok.to_csv('ok.csv')
    #df_all.to_json('prueba.json', orient='records', lines=True)

def Caso2_revisar(**context):
    manual = """
            Esta funcion determina los registros que hay que revisar entre el NE y el inventario, por tener estados inconsistentes.
    Args: 
      none
    Returns:
      none
    """
    import pandas as pd

    table_A = 'ne'
    table_B = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    #casos Revisar:
    df_rev1 = pd.read_sql_query("""select * from {0} WHERE concat IN 
                                (
                                select concat from {0} a where portoperationalstate = 'up'
                                INTERSECT
                                select concat from {1} b where portoperationalstate IN ('Available', 'Planned', 'Reserved', 'Undefined', 'Seems to be deleted')
                                )"""
                                .format(table_A,table_B),con=conn)

    df_rev2 = pd.read_sql_query("""select * from {0} WHERE concat IN 
                                (
                                select concat from {0} a where portoperationalstate = 'down'
                                INTERSECT
                                select concat from {1} b where portoperationalstate IN ('Active')
                                )"""
                                .format(table_A,table_B),con=conn)

    df_rev = pd.concat ([df_rev1,df_rev2])

    conn.close()

    print (len(df_rev))
    df_rev.to_csv('rev.csv')

def Caso3_ne_inv(**context):
    manual = """
            Esta funcion determina los registros que existen en el NE y NO existen en el inventario.
    Args: 
      none
    Returns:
      none
    """
    import pandas as pd

    table_A = 'ne'
    table_B = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    #existencia:
    #estan en NE y no estan en inventario:
    df_ex_ne_inv = pd.read_sql_query("""select * from {0} WHERE concat IN
                                (
                                select concat from {0} a 
                                EXCEPT
                                select concat from {1} b
                                )"""
                                .format(table_A,table_B),con=conn)
    
    conn.close()
    print(len(df_ex_ne_inv))
    df_ex_ne_inv.to_csv('ex_ne_inv.csv')

def Caso4_inv_ne(**context):
    manual = """
            Esta funcion determina los registros que existen en el Inventario y NO existen en el NE.
    Args: 
      none
    Returns:
      none
    """
    import pandas as pd

    table_A = 'ne'
    table_B = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    #existencia:
    #estan en inventario y no estan en NE:
    df_ex_inv_ne = pd.read_sql_query("""select * from {1} where concat IN
                                (
                                select concat from {1} a 
                                EXCEPT
                                select concat from {0} b)"""
                                .format(table_A,table_B),con=conn)
    print(len(df_ex_inv_ne))

    conn.close()
    df_ex_inv_ne.to_csv('ex_inv_ne.csv')


#########################################################


#tasks
_extrae_bd_inventario = DummyOperator(task_id='Extrae_bd_inventario', retries=1, dag=dag)

_extrae_bd_NE = DummyOperator(task_id='Extrae_bd_NE', retries=1, dag=dag)

_carga_inv_to_db = PythonOperator(
    task_id='Carga_inv_to_db',
    python_callable=Load_inv,
    op_kwargs={
        'file':'Table-id_2225467905.csv',
        'dir':'Inner',
        'table':'inv_itf'
        },
    provide_context=True,
    dag=dag
)

_carga_ne_to_db = PythonOperator(
    task_id='Carga_ne_to_db',
    python_callable=Load_inv,
    op_kwargs={    
        #'file':'huawei_IC1.HOR1_interfaces.txt',
        'file':'huawei_IC1.SLO1_interfaces.txt',
        'dir':'Inner/cu1/interfaces',
        'table':'ne'
        },
    provide_context=True,
    dag=dag
)

_adecuar_naming_inv = PythonOperator(
    task_id='Adecuar_naming_inventario', 
    python_callable=naming_inv,
    dag=dag)

_adecuar_naming_ne = PythonOperator(
    task_id='Adecuar_naming_ne', 
    python_callable=naming_ne,
    dag=dag)

_caso1 = PythonOperator(
    task_id='Caso1_Registros_ok', 
    python_callable=Caso1_ok,
    retries=1, dag=dag
    )
_caso2 = PythonOperator(
    task_id='Caso2_Revisar_Registros', 
    python_callable=Caso2_revisar,
    retries=1, dag=dag
    )

_caso3 = PythonOperator(
    task_id='Caso3_ExisteNE_NoExisteInv', 
    python_callable=Caso3_ne_inv,
    retries=1, dag=dag
    )

_caso4 = PythonOperator(
    task_id='Caso4_ExisteInv_NoExisteNE', 
    python_callable=Caso4_inv_ne,
    retries=1, dag=dag
    )


_naming_adecuados = DummyOperator(task_id='Naming_Adecuados', retries=1, dag=dag)

_imprime_reporte1 = DummyOperator(task_id='Imprime_reporte1', retries=1, dag=dag)

_imprime_reporte2 = DummyOperator(task_id='Imprime_reporte2', retries=1, dag=dag)

_imprime_reporte3 = DummyOperator(task_id='Imprime_reporte3', retries=1, dag=dag)

#Secuencia
_extrae_bd_inventario >> _carga_inv_to_db >> _adecuar_naming_inv >> _naming_adecuados

_extrae_bd_NE >> _carga_ne_to_db >> _adecuar_naming_ne >> _naming_adecuados

_naming_adecuados >> _caso1 >> _imprime_reporte1

_naming_adecuados >> _caso2 >> _imprime_reporte2

_naming_adecuados >> _caso3 >> _imprime_reporte2

_naming_adecuados >> _caso4 >> _imprime_reporte3