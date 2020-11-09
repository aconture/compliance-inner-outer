from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks import PostgresHook
from time import sleep
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from lib.teco_db import *
from lib.teco_callelements import *
import lib.teco_reports


#####################################################################
#####################################################################
#arg

default_args = {

    'owner': 'MVP.In-house',
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

#####################################################################
#####################################################################
#dag

dag = DAG(
    dag_id='Compliance_Inner_Outer-MODULAR', 
    schedule_interval= None,
    tags=['inventario', 'compliance'],
    default_args=default_args
)

#####################################################################
#####################################################################
#Funciones propietarias del caso de uso

def naming_inv(**context):
    
    manual = """
    Esta funcion modifica el contenido de ciertos campos traidos desde el inventario para que puedan ser comparados con el archivo que trae ansible desde el NE.
    La lectura la realiza de la tabla inv_itf.
    El resultado lo guarda en la tabla par_inv_itf. Esta tabla va a ser un subset de la tabla origen, que va atener solamente los registros que son comparados de acuerdo al 'networkrole'.

    Args: 
      none 
    Returns:
      none
    
    """

    table_ = 'inv_itf'
    table_dest = 'par_inv_itf'

    #Inicializamos la base
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    df_inv_itf = pd.read_sql_query('select * from {}'.format(table_),con=conn)

    #filtro1: lo aplico para que la tarea corra más rápido
    df_inv_itf = df_inv_itf[(df_inv_itf['shelfnetworkrole'] == 'INNER CORE') | (df_inv_itf['shelfnetworkrole'] == 'OUTER CORE')]
    
    #inicializo adecuaciones
    ic_mod_label = 0
    ic_mod_100GE = 0
    ic_mod_GE = 0
    oc_mod_100GE = 0
    oc_mod_10Gb = 0

    #print (df_inv_itf.columns)
    for indice in df_inv_itf.index:
        #adecuaciones especificas para el Inner Core
        if (df_inv_itf.loc[indice,'shelfnetworkrole'] == 'INNER CORE'):
            if (df_inv_itf.loc[indice,'portbandwidth'] == '10 Gb'):
                df_inv_itf.loc[indice,'portinterfacename'] = df_inv_itf.loc[indice,'portinterfacename']+'(100M)'
                ic_mod_label = ic_mod_label + 1
            if (df_inv_itf.loc[indice,'portbandwidth'] == '100 Gb'):
                df_inv_itf.loc[indice,'portbandwidth'] = '100GE'
                ic_mod_100GE = ic_mod_100GE + 1
            if (df_inv_itf.loc[indice,'portbandwidth'] == '10 Gb'):
                df_inv_itf.loc[indice,'portbandwidth'] = 'GE'
                ic_mod_GE = ic_mod_GE + 1
    
        #adecuaciones especificas para el Outer Core
        if (df_inv_itf.loc[indice,'shelfnetworkrole'] == 'OUTER CORE'):
            if ((df_inv_itf.loc[indice,'portbandwidth'] == '100 Gb') or (df_inv_itf.loc[indice,'portbandwidth'] == '100GB')):
                df_inv_itf.loc[indice,'portbandwidth'] = 'Hu'
                oc_mod_100GE = oc_mod_100GE + 1
            if (df_inv_itf.loc[indice,'portbandwidth'] == '10 Gb'):
                df_inv_itf.loc[indice,'portbandwidth'] = 'Te'
                oc_mod_10Gb = oc_mod_10Gb + 1

    logging.info ('\n::: Labels (100M) modificados para Inner Core: {0}'.format(ic_mod_label))
    logging.info ('\n::: Label 100GE modificados para Inner Core: {0}'.format(ic_mod_100GE))
    logging.info ('\n::: Label GE modificados para Inner Core: {0}'.format(ic_mod_GE))
    logging.info ('\n::: Labels 100 Gb modificados para Outer Core: {0}'.format(oc_mod_100GE))
    logging.info ('\n::: Labels 10 Gb modificados para Outer Core: {0}'.format(oc_mod_10Gb))


    #Adecuaciones masivas
    df_inv_itf['concat'] = df_inv_itf[['shelfname','portbandwidth','portinterfacename']].agg(''.join, axis=1)


    #init de la base destino
    sql_delete = 'DELETE FROM {}'.format(table_dest)
    lib.teco_db._delete_cursor(sql_delete)
    logging.info('\n::: Tabla {} inicializada.'.format(table_dest))

    #populo la base destino
    columnas = df_inv_itf.columns.ravel()
    lib.teco_db._insert_cursor(df_inv_itf,table_dest,columnas)
    

#####################################################################

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
    lib.teco_db._delete_cursor(sql_delete)
    logging.info('\n::: Tabla {} inicializada.'.format(table_dest))

    #populo la base destino
    columnas = df_ne.columns.ravel()
    lib.teco_db._insert_cursor(df_ne,table_dest,columnas)
    

#####################################################################

def Caso_ok_v2(**context):

    manual = """
    Esta funcion determina los registros correctamente sincronizados entre el NE y el inventario.

    Estados registrados como 'ok': 
    (Inventario.portOperationalState, ne.portoperationalstate, ne.protocol)
    Active	    up	    up|down
    Available	down	down
    Reserved	down	down

    Args: 
      none
    Returns:
      none

    """

    table_A = 'ne'
    table_B = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    df_ok = pd.DataFrame()
    df_complemento = pd.DataFrame()

    #los valores que hacen True la condicion 'ok':
    struct = {'condiciones':
        [
            {'inv.portoperationalstate':'Active','ne.portoperationalstate':'up','ne.protocol':'up'},
            {'inv.portoperationalstate':'Active','ne.portoperationalstate':'up','ne.protocol':'down'},
            {'inv.portoperationalstate':'Available','ne.portoperationalstate':'down','ne.protocol':'down'},
            {'inv.portoperationalstate':'Reserved','ne.portoperationalstate':'down','ne.protocol':'down'},
        ]
    }

    #itero la base para cada condicion:
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {0} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_ok = pd.concat([df_ok,df_iter])
        logging.info ('Registros ok de la condicion {0}: {1}'.format(idx,len(df_iter)))

    #a los campos que traje con la table 'ne' agrego los campos que obtengo de la tabla de inventario
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {3} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_complemento = pd.concat([df_complemento,df_iter])

    df_ok = pd.merge(df_ok,df_complemento, how='left', on='concat')

    df_ok['EvEstado'] = 'ok'
        
    df_ok = lib.teco_reports._format_reporte_compliance(df_ok)

    #print (df_ok.columns.ravel())

    conn.close()
    
    logging.info ('\n:::Registros ok totales: {} \n'.format(len(df_ok)))

    df_ok.to_csv('reports/auxiliar/ok.csv', index=False)
    #df_all.to_json('prueba.json', orient='records', lines=True)

#####################################################################

def Caso2_revisar_1(**context):

    manual = """
    Esta funcion determina los registros que hay que revisar entre el NE y el inventario, por tener estados inconsistentes.
    Args: 
      none
    Returns:
      none
    
    """
    table_A = 'ne'
    table_B = 'par_inv_itf'

    #los valores que hacen True la condicion 'revisar':
    struct = {'condiciones':
        [
            {'inv.portoperationalstate':'Available','ne.portoperationalstate':'up','ne.protocol':'up'},
            {'inv.portoperationalstate':'Available','ne.portoperationalstate':'up','ne.protocol':'down'},
            {'inv.portoperationalstate':'Planned','ne.portoperationalstate':'up','ne.protocol':'up'},
            {'inv.portoperationalstate':'Planned','ne.portoperationalstate':'up','ne.protocol':'down'},
            {'inv.portoperationalstate':'Planned','ne.portoperationalstate':'down','ne.protocol':'down'},            
            {'inv.portoperationalstate':'Undefined','ne.portoperationalstate':'up','ne.protocol':'up'},
            {'inv.portoperationalstate':'Undefined','ne.portoperationalstate':'up','ne.protocol':'down'},            
            {'inv.portoperationalstate':'Seems to be deleted','ne.portoperationalstate':'up','ne.protocol':'up'},
            {'inv.portoperationalstate':'Seems to be deleted','ne.portoperationalstate':'up','ne.protocol':'down'},
        ]
    }

    df_ok = pd.DataFrame()
    df_complemento = pd.DataFrame()

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()


    #itero la base para cada condicion:
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {0} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_ok = pd.concat([df_ok,df_iter])
        logging.info ('Registros ok de la condicion {0}: {1}'.format(idx,len(df_iter)))

    #a los campos que traje con la table 'ne' agrego los campos que obtengo de la tabla de inventario
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {3} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_complemento = pd.concat([df_complemento,df_iter])

    df_ok = pd.merge(df_ok,df_complemento, how='left', on='concat')

    df_ok['EvEstado'] = 'revisar_1'
    #print (df_rev.columns)
    df_rev = lib.teco_reports._format_reporte_compliance(df_ok)

    conn.close()
    logging.info ('\n:::Registros a revisar: {}'.format(len(df_ok)))
    df_rev.to_csv('reports/auxiliar/rev_1.csv', index=False)


#####################################################################

def Caso2_revisar_2(**context):

    manual = """
    Esta funcion determina los registros que hay que revisar entre el NE y el inventario, por tener estados inconsistentes.
    Args: 
      none
    Returns:
      none
    
    nota aux: elementos variables: struct, nombre_estado, nombre_archivo_csv
    """
    table_A = 'ne'
    table_B = 'par_inv_itf'

    #los valores que hacen True la condicion 'revisar':
    struct = {'condiciones':
        [
            {'inv.portoperationalstate':'Undefined','ne.portoperationalstate':'up','ne.protocol':'up'},
            {'inv.portoperationalstate':'Seems to be deleted','ne.portoperationalstate':'down','ne.protocol':'down'},
            {'inv.portoperationalstate':'Active','ne.portoperationalstate':'down','ne.protocol':'down'},            
        ]
    }

    df_ok = pd.DataFrame()
    df_complemento = pd.DataFrame()

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()


    #itero la base para cada condicion:
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {0} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_ok = pd.concat([df_ok,df_iter])
        logging.info ('Registros ok de la condicion {0}: {1}'.format(idx,len(df_iter)))

    #a los campos que traje con la table 'ne' agrego los campos que obtengo de la tabla de inventario
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {3} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_complemento = pd.concat([df_complemento,df_iter])

    df_ok = pd.merge(df_ok,df_complemento, how='left', on='concat')

    df_ok['EvEstado'] = 'revisar_1'
    #print (df_rev.columns)
    df_rev = lib.teco_reports._format_reporte_compliance(df_ok)

    conn.close()
    logging.info ('\n:::Registros a revisar: {}'.format(len(df_ok)))
    df_rev.to_csv('reports/auxiliar/rev_2.csv', index=False)

#####################################################################

def Caso_ok_reserva(**context):

    manual = """
    Esta funcion determina los registros .
    Args: 
      none
    Returns:
      none
    
    nota aux: elementos variables: struct, nombre_estado, nombre_archivo_csv
    """
    table_A = 'ne'
    table_B = 'par_inv_itf'

    #los valores que hacen True la condicion 'revisar':
    struct = {'condiciones':
        [
            {'inv.portoperationalstate':'Reserved','ne.portoperationalstate':'up','ne.protocol':'up'},
            {'inv.portoperationalstate':'Reserved','ne.portoperationalstate':'up','ne.protocol':'down'},
        ]
    }

    df_ok = pd.DataFrame()
    df_complemento = pd.DataFrame()

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()


    #itero la base para cada condicion:
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {0} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_ok = pd.concat([df_ok,df_iter])
        logging.info ('Registros ok de la condicion {0}: {1}'.format(idx,len(df_iter)))

    #a los campos que traje con la table 'ne' agrego los campos que obtengo de la tabla de inventario
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {3} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_complemento = pd.concat([df_complemento,df_iter])

    df_ok = pd.merge(df_ok,df_complemento, how='left', on='concat')

    df_ok['EvEstado'] = 'ok_reserva'
    #print (df_rev.columns)
    df_rev = lib.teco_reports._format_reporte_compliance(df_ok)

    conn.close()
    logging.info ('\n:::Registros a revisar: {}'.format(len(df_ok)))
    df_rev.to_csv('reports/auxiliar/ok_reserva.csv', index=False)

#####################################################################

def Caso_na(**context):

    manual = """
    Esta funcion determina los registros que tienen un estado N/A.
    Args: 
      none
    Returns:
      none
    
    nota aux: elementos variables: struct, nombre_estado, nombre_archivo_csv
    """
    table_A = 'ne'
    table_B = 'par_inv_itf'

    #los valores que hacen True la condicion 'revisar':
    struct = {'condiciones':
        [
            {'inv.portoperationalstate':'Available','ne.portoperationalstate':'down','ne.protocol':'up'},
            {'inv.portoperationalstate':'Planned','ne.portoperationalstate':'down','ne.protocol':'up'},
            {'inv.portoperationalstate':'Reserved','ne.portoperationalstate':'down','ne.protocol':'up'},
            {'inv.portoperationalstate':'Undefined','ne.portoperationalstate':'down','ne.protocol':'up'},
            {'inv.portoperationalstate':'Seems to be deleted','ne.portoperationalstate':'down','ne.protocol':'up'},            
            {'inv.portoperationalstate':'Active','ne.portoperationalstate':'down','ne.protocol':'up'},            
        ]
    }

    df_ok = pd.DataFrame()
    df_complemento = pd.DataFrame()

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()


    #itero la base para cada condicion:
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {0} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_ok = pd.concat([df_ok,df_iter])
        logging.info ('Registros ok de la condicion {0}: {1}'.format(idx,len(df_iter)))

    #a los campos que traje con la table 'ne' agrego los campos que obtengo de la tabla de inventario
    for idx in range (0, len(struct['condiciones'])):
        #print (struct['condiciones'][idx]['ne.portoperationalstate'])
        df_iter = pd.read_sql_query("""select * from {3} WHERE concat IN
                                    (
                                    select concat from {0} a where 
                                        (portoperationalstate = \'{1}\' and protocol = \'{2}\')
                                    INTERSECT
                                    select concat from {3} b where portoperationalstate = \'{4}\'
                                    )"""
                                    .format(
                                        table_A,
                                        struct['condiciones'][idx]['ne.portoperationalstate'],
                                        struct['condiciones'][idx]['ne.protocol'],
                                        table_B,
                                        struct['condiciones'][idx]['inv.portoperationalstate']),
                                        con=conn)

        df_complemento = pd.concat([df_complemento,df_iter])

    df_ok = pd.merge(df_ok,df_complemento, how='left', on='concat')

    df_ok['EvEstado'] = 'n_a'
    #print (df_rev.columns)
    df_rev = lib.teco_reports._format_reporte_compliance(df_ok)

    conn.close()
    logging.info ('\n:::Registros a revisar: {}'.format(len(df_ok)))
    df_rev.to_csv('reports/auxiliar/na.csv', index=False)


#####################################################################

def Caso3_ne_inv(**context):

    manual = """

    Esta funcion determina los registros que existen en el NE y NO existen en el inventario.
    Args: 
      none
    Returns:
      none

    """
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

    #print (df_ex_ne_inv)
    df_ex_ne_inv['EvEstado'] = 'Falta_en_inventario'

    #Con esto formateo los campos para que en el excel pueda usar una unica solapa de este resultado junto con los resultados de ok y revisar
    df_ex_ne_inv['shelfname_x'] = df_ex_ne_inv['shelfname']
    df_ex_ne_inv['portoperationalstate_x'] = df_ex_ne_inv['portoperationalstate']
    df_ex_ne_inv['portoperationalstate_y'] = 'N/A' #estado desconocido en el inventario
    #df_ex_ne_inv['info1_x'] = df_ex_ne_inv['info1']
    df_ex_ne_inv['portinfo1'] = 'N/A'

    #voy a tener que llamar a esta función explicitamente para cada networkrole para poder popular los siguientes campos:
    df_ex_ne_inv['shelfNetworkRole'] = '0-Crear en Inventario'
    df_ex_ne_inv['shelfHardware'] = 'N/A' #nombre del campo del dump de Lisy

    df_ex_ne_inv['portBandwidth'] = 'N/A' #la conformacion de este dato requiere desarrollo adicional

    df_ex_ne_inv = lib.teco_reports._format_reporte_compliance(df_ex_ne_inv)

    logging.info ('\n:::Registros existentes en NE y faltan en Inventario: {}'.format(len(df_ex_ne_inv)))

    #_gen_excel(df_ex_ne_inv,'FaltaEnInv')
    df_ex_ne_inv.to_csv('reports/auxiliar/df_ex_ne_inv.csv', index=False)


#####################################################################

def Caso_inv_ne(**context):

    manual = """

    Esta funcion determina los registros que existen en el Inventario y NO existen en el NE.
    Args: 
      none
    Returns:
      none

    """
    rol=context['role']

    table_A = 'ne'
    table_B = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    #existencia:
    #estan en inventario y no estan en NE:
    df_ex_inv_ne = pd.read_sql_query("""select * from {1} where concat IN
                                (
                                select concat from {1} a where shelfnetworkrole = 'INNER CORE'
                                EXCEPT
                                select concat from {0} b)"""
                                .format(table_A,table_B),con=conn)
    print(len(df_ex_inv_ne))
    
    logging.info ('\n:::Registros existentes en Inventario y faltan en NE: {}'.format(len(df_ex_inv_ne)))

    conn.close()
    df_ex_inv_ne.to_csv('reports/auxiliar/ex_inv_ne.csv', index=False)


#####################################################################
#####################################################################
#tasks

_extrae_bd_inventario = DummyOperator(task_id='Extrae_bd_inventario', retries=1, dag=dag)

_auto_ansible = PythonOperator(
    task_id='ejecuta_ansible', 
    python_callable=lib.teco_callelements.call_ansible,
    op_kwargs={
        'connection':'ansible_proxy'
        },
    dag=dag)

_extrae_bd_NE = PythonOperator(
    task_id='trae_archivos', 
    python_callable=lib.teco_callelements.scp_files,
    op_kwargs={
        'connection':'ansible_proxy',
        'local_dir':'/usr/local/airflow/Inner/cu1/interfaces/',
        'remote_dir':'/usr/local/ansible/mejoras_cu1/interfaces/'
        },
    dag=dag)

_carga_inv_to_db = PythonOperator(
    task_id='Carga_inv_to_db',
    python_callable=Load_inv,
    op_kwargs={
        'file':['EthernetPortsByIpShelf.txt'],
        'dir':'Inner',
        'role': '*',
        'table':'inv_itf',
        'datatype':'csv'
        },
    provide_context=True,
    dag=dag
)

_carga_ne_to_db = PythonOperator(
    task_id='Carga_ne_to_db',
    python_callable=Load_inv,
    op_kwargs={    
        'file':['*'],
        'dir':'Inner/cu1/interfaces',
        'role': '*',
        'table':'ne',
        'datatype':'csv'        
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
    task_id='Registros_ok', 
    python_callable=Caso_ok_v2,
    retries=1, dag=dag
    )
_caso2 = PythonOperator(
    task_id='A_Revisar_Prioritarios', 
    python_callable=Caso2_revisar_1,
    retries=1, dag=dag
    )

_caso3 = PythonOperator(
    task_id='Caso3_ExisteNE_NoExisteInv', 
    python_callable=Caso3_ne_inv,
    retries=1, dag=dag
    )

_caso4 = PythonOperator(
    task_id='A_Revisar_menos_prioritarios', 
    python_callable=Caso2_revisar_2,
    retries=1, dag=dag
    )

_caso5 = PythonOperator(
    task_id='ok_con_reserva', 
    python_callable=Caso_ok_reserva,
    retries=1, dag=dag
    )


_caso6 = PythonOperator(
    task_id='estados_N-A', 
    python_callable=Caso_na,
    retries=1, dag=dag
    )

"""
_caso4 = PythonOperator(
    task_id='Caso4_ExisteInv_NoExisteNE', 
    op_kwargs={    
    'role':'INNER CORE',
    },
    python_callable=Caso_inv_ne,
    retries=1, dag=dag
    )
"""

_init_reporting = PythonOperator(
    task_id='Init_Reporting',
    op_kwargs={    
    'dir':'reports',
    },
    python_callable=lib.teco_reports.init_report,
    retries=1, dag=dag)

_imprime_reporte = PythonOperator(
    task_id='Genera_Reporte',
    op_kwargs={    
    'dir':'reports',
    },
    python_callable=lib.teco_reports.gen_excel,
    retries=1, dag=dag)

_envia_mail1 = EmailOperator(
    task_id='Email_to_canal',
    to="6b8581f6.teco.com.ar@amer.teams.ms",
    #to="c23383e8.teco.com.ar@amer.teams.ms", #canal teams de in-house
    subject="Compliance Inner&Outer - Resultado de Ejecucion {{ ds }}",
    html_content=lib.teco_reports._cuerpo_mail(),
    files=["/usr/local/airflow/reports/reporte.xlsx"],
    dag=dag
)

#####################################################################
#####################################################################
#Secuencia

_extrae_bd_inventario >> _carga_inv_to_db >> _adecuar_naming_inv >> _init_reporting

_auto_ansible >> _extrae_bd_NE >> _carga_ne_to_db >> _adecuar_naming_ne >> _init_reporting

_init_reporting >> _caso1 >> _imprime_reporte

_init_reporting >> _caso2 >> _imprime_reporte

_init_reporting >> _caso3 >> _imprime_reporte

_init_reporting >> _caso4 >> _imprime_reporte

_init_reporting >> _caso5 >> _imprime_reporte

_init_reporting >> _caso6 >> _imprime_reporte

_imprime_reporte >> _envia_mail1
