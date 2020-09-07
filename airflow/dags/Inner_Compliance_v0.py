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

def call_ansible(**context):
    manual = """
    Args: 
      connection [text]: id de la conexion creada en airflow (Admin->Connections), donde:
        "host":"ip o hostname del ansible proxy"
        "user":"id del usuario existente en el ansible proxy"
        "pass":"pass del usuario en el ansible proxy"
    """

    from airflow.hooks.base_hook import BaseHook

    try:
        conn_id = context['connection']
    except:
        logging.error ('\n\n:::! Error - Falta un argumento de llamada a esta funcion.')
        logging.error (manual)
        return -1

    try:
        connection = BaseHook.get_connection(conn_id)
        host = connection.host
        user = connection.login
        passw = connection.password

        #la linea siguiente la comento para no romper produccion
        #os.system('sshpass -e ssh u565589@10.9.44.173 \'rm /home/u565589/desarrollo/irs_cu/mejoras_cu1/interfaces/*.txt; cd /home/u565589/desarrollo/irs_cu/mejoras_cu1/yaml/; ansible-playbook main.yaml\'')
        os.system ('sshpass -p {0} ssh {1}@{2} \'rm /home/u565589/desarrollo/irs_cu/mejoras_cu1/interfaces/*.txt; cd /home/u565589/desarrollo/irs_cu/mejoras_cu1/yaml/; ansible-playbook main.yaml\''.format(passw,user,host))
        #os.system ('sshpass -p {0} ssh {1}@{2} \'pwd; ls -lrt\''.format(passw,user,host))
    except:
        logging.error ('\n\n:::! Problema en la conexión al servidor remoto.\n')
        return -1
    
def scp_files(**context):
    manual = """
    Args: 
      connection [text]: id de la conexion creada en airflow (Admin->Connections), donde:
        "host":"ip o hostname del ansible proxy"
        "user":"id del usuario existente en el ansible proxy"
        "pass":"pass del usuario en el ansible proxy"
      
      local_dir [text]: path absoluto del directorio remoto.
        ej: '/usr/local/airflow/Inner/'

      remote_dir [text]: path absoluto del directorio remoto.
        ej: '/home/u123456/'

    Return:
      -1 si error
      none si exitoso
    """
    from airflow.hooks.base_hook import BaseHook

    try:
        conn_id = context['connection']
        local_dir = context['local_dir']
        remote_dir = context['remote_dir']
    except:
        logging.error ('\n\n:::! Error - Falta un argumento de llamada a esta funcion.\n')
        logging.error (manual)
        return -1

    try:
        connection = BaseHook.get_connection(conn_id)
        host = connection.host
        user = connection.login
        passw = connection.password

        os.system('rm {}*.txt'.format(local_dir))
        logging.info ('::: Inicializado el directorio de *.txt local: {0}'.format(local_dir))
        
        #os.system('sshpass -e scp u565589@10.9.44.173:/home/u565589/desarrollo/irs_cu/mejoras_cu1/interfaces/*.txt /usr/local/airflow/Inner/cu1/interfaces')
        logging.info ('::: Trayendo archivos *.txt del directorio ansible remoto')
        os.system('sshpass -p {0} scp {1}@{2}:{3}*.txt {4}'.format(passw,user,host,remote_dir,local_dir))
        logging.info ('::: Archivos *.txt ansible copiados al directorio local')

    except:
        logging.error (':::! Problema en la conexión al servidor remoto.\n')
        return -1

def Load_inv(**context):
    manual = """
    Esta funcion carga los registros leidos del archivo indicado en la base de postgres
    
    Args: 
      dir [text]: directorio dentro del home de Airflow, donde se encuentra el archivo o el grupo de archivos a cargar. No debe incluir '/' al final.
      role [text]: rol definido en el inventario
      table [text]: tabla de la base de datos que se debe cargar.
      file [text]: archivo con la base a cargar. Si su valor = '*', se cargan todos los archivos del directorio indicado.      
    
    Returns:
      none
    """
    from psycopg2.extras import execute_values
    import pandas as pd

    file=context['file']
    dir=context['dir']
    table=context['table']
    rol=context['role']

    if (dir is None) or (table is None) or (rol is None) or (file is None):
        logging.info ('\n:::! Error - Falta un argumento de llamada a esta funcion.')
        logging.info (manual)
        return -1

    if file == '*':
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
            if rol != '*':
                try:
                  df = df[(df['NetworkRole'] == rol)] #filtro el rol de la base traida del inventario
                except:
                  pass
            logging.info ('\n::: Cargando desde Archivo{0}, {1} registros con el rol \'{2}\'.'.format(nom_archivo,len(df), rol))
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
    ic_mod_label = 0
    ic_mod_100GE = 0
    ic_mod_GE = 0
    oc_mod_100GE = 0
    oc_mod_10Gb = 0

    #print (df_inv_itf.columns)
    for indice in df_inv_itf.index:
        if (df_inv_itf.loc[indice,'networkrole'] == 'INNER CORE'):
            if (df_inv_itf.loc[indice,'bandwidth'] == '10 Gb'):
                df_inv_itf.loc[indice,'userlabel'] = df_inv_itf.loc[indice,'userlabel']+'(100M)'
                #print (df_inv_itf.loc[indice,'userlabel'])
                ic_mod_label = ic_mod_label + 1
            if (df_inv_itf.loc[indice,'bandwidth'] == '100 Gb'):
                df_inv_itf.loc[indice,'bandwidth'] = '100GE'
                ic_mod_100GE = ic_mod_100GE + 1
            if (df_inv_itf.loc[indice,'bandwidth'] == '10 Gb'):
                df_inv_itf.loc[indice,'bandwidth'] = 'GE'
                ic_mod_GE = ic_mod_GE + 1
    
        #adecuaciones especificas para el Outer Core
        if (df_inv_itf.loc[indice,'networkrole'] == 'OUTER CORE'):
            if ((df_inv_itf.loc[indice,'bandwidth'] == '100 Gb') or (df_inv_itf.loc[indice,'bandwidth'] == '100GB')):
                df_inv_itf.loc[indice,'bandwidth'] = 'Hu'
                oc_mod_100GE = oc_mod_100GE + 1
            if (df_inv_itf.loc[indice,'bandwidth'] == '10 Gb'):
                df_inv_itf.loc[indice,'bandwidth'] = 'Te'
                oc_mod_10Gb = oc_mod_10Gb + 1

    logging.info ('\n::: Labels (100M) modificados para Inner Core: {0}'.format(ic_mod_label))
    logging.info ('\n::: Label 100GE modificados para Inner Core: {0}'.format(ic_mod_100GE))
    logging.info ('\n::: Label GE modificados para Inner Core: {0}'.format(ic_mod_GE))
    logging.info ('\n::: Labels 100 Gb modificados para Outer Core: {0}'.format(oc_mod_100GE))
    logging.info ('\n::: Labels 10 Gb modificados para Outer Core: {0}'.format(oc_mod_10Gb))


    #Adecuaciones masivas
    #df_inv_itf['bandwidth'] = df_inv_itf['bandwidth'].str.replace('100 Gb','100GE')
    #df_inv_itf['bandwidth'] = df_inv_itf['bandwidth'].str.replace('10 Gb','GE')
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

def gen_excel(**context):
    manual = """
    Esta funcion genera un archivo excel con el contenido de los 'n' archivos csv que lee en el directorio. Requiere que todos los archivos csv tengan los mismos campos, y que el archivo excel no existe previamente, para no duplicar el nombre de la solapa. 

    Args: 
      none
    Returns:
      none
    """

    import openpyxl
    import pandas as pd

    dir=context['dir']
    archivos=os.listdir(os.path.join(os.getcwd(),dir,'auxiliar'))
    print (archivos)

    dataframe = pd.DataFrame()
    for nom_archivo in archivos:
        """
        abspath = os.path.join(os.getcwd(),dir,'auxiliar',nom_archivo)
        solapa = os.path.basename(abspath)
        dataframe = pd.read_csv(abspath,delimiter=',')
        archivo_rep = os.path.join(os.getcwd(),dir,'reporte.xlsx')
        #print (solapa)
        """
        #la proxima version:
        abspath = os.path.join(os.getcwd(),dir,'auxiliar',nom_archivo)
        dataframe_aux = pd.read_csv(abspath,delimiter=',')
        dataframe = pd.concat ([dataframe,dataframe_aux])
        
    #print (dataframe)
    archivo_rep = os.path.join(os.getcwd(),dir,'reporte.xlsx')        
    try:
        with pd.ExcelWriter(archivo_rep,mode='a',engine='openpyxl', encoding="utf-8-sig") as escritor:
            dataframe.to_excel(escritor, sheet_name='crudo', index=None)
    except FileNotFoundError:
        with pd.ExcelWriter(archivo_rep,mode='n',engine='openpyxl', encoding="utf-8-sig") as escritor:
            dataframe.to_excel(escritor, sheet_name='crudo', index=None)
    finally:
        escritor.save

def init_report(**context):
    import os
    dir=context['dir']
    try:
        os.remove('reports/reporte.xlsx')
    except FileNotFoundError:
        pass

def _format_reporte(dataframe):
    dataframe = dataframe.rename(columns={
      'portoperationalstate_x':'EstadoRed',
      'portoperationalstate_y': 'EstadoLisy',
      'info1_x':'DescRed',
      'info1_y':'DescLisy',
      'shelfname_x':'NE',
      'concat':'Recurso'
      })
    
    dataframe = dataframe [[
      'networkrole',
      'NE',
      'hardware',
      'interface',
      'Recurso',
      'bandwidth',
      'EstadoRed',
      'EstadoLisy',
      'DescRed',
      'DescLisy',
      'EvEstado'
    ]]
    return(dataframe)

def Caso1_ok_v2(**context):
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

    df_ok1_complemento = pd.read_sql_query("""select * from {1} WHERE concat IN
                                (
                                select concat from {0} a where portoperationalstate = 'up'
                                INTERSECT
                                select concat from {1} b where portoperationalstate IN ('Active')
                                )"""
                                .format(table_A,table_B),con=conn)

    df_ok2_complemento = pd.read_sql_query("""select * from {1} WHERE concat IN 
                                (
                                select concat from {0} a where portoperationalstate = 'down'
                                INTERSECT
                                select concat from {1} b where portoperationalstate NOT IN ('Active')
                                )"""
                                .format(table_A,table_B),con=conn)

    df_ok2 = pd.merge(df_ok2,df_ok2_complemento, how='left', on='concat')
    df_ok1 = pd.merge(df_ok1,df_ok1_complemento, how='left', on='concat')

    df_ok = pd.concat ([df_ok1,df_ok2])

    df_ok['EvEstado'] = 'ok'
    df_ok = _format_reporte(df_ok)

    #print (df_ok.columns.ravel())

    conn.close()
    
    logging.info ('\n:::Registros ok: {}'.format(len(df_ok)))

    #_gen_excel(df_ok,'ok')

    #impresiones:
    print (len(df_ok))

    df_ok.to_csv('reports/auxiliar/ok.csv')
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

    df_rev1_complemento = pd.read_sql_query("""select * from {1} WHERE concat IN 
                                (
                                select concat from {0} a where portoperationalstate = 'up'
                                INTERSECT
                                select concat from {1} b where portoperationalstate IN ('Available', 'Planned', 'Reserved', 'Undefined', 'Seems to be deleted')
                                )"""
                                .format(table_A,table_B),con=conn)

    df_rev2_complemento = pd.read_sql_query("""select * from {1} WHERE concat IN 
                                (
                                select concat from {0} a where portoperationalstate = 'down'
                                INTERSECT
                                select concat from {1} b where portoperationalstate IN ('Active')
                                )"""
                                .format(table_A,table_B),con=conn)

    df_rev1 = pd.merge(df_rev1,df_rev1_complemento, how='left', on='concat')
    df_rev2 = pd.merge(df_rev2,df_rev2_complemento, how='left', on='concat')

    df_rev = pd.concat ([df_rev1,df_rev2])

    df_rev['EvEstado'] = 'revisar'
    df_rev = _format_reporte(df_rev)

    conn.close()

    logging.info ('\n:::Registros a revisar: {}'.format(len(df_rev)))

    #_gen_excel(df_rev,'revisar')

    #print (len(df_rev))
    df_rev.to_csv('reports/auxiliar/rev.csv')

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

    df_ex_ne_inv['EvEstado'] = 'Falta_en_inventario'

    #Con esto formateo los campos para que en el excel pueda usar una unica solapa de este resultado junto con los resultados de ok y revisar
    df_ex_ne_inv['shelfname_x'] = df_ex_ne_inv['shelfname']
    df_ex_ne_inv['portoperationalstate_x'] = df_ex_ne_inv['portoperationalstate']
    df_ex_ne_inv['portoperationalstate_y'] = 'N/A' #estado desconocido en el inventario
    df_ex_ne_inv['info1_x'] = df_ex_ne_inv['info1']
    df_ex_ne_inv['info1_y'] = 'N/A'

    #voy a tener que llamar a esta función explicitamente para cada networkrole para poder popular los siguientes campos:
    df_ex_ne_inv['networkrole'] = 'N/A'
    df_ex_ne_inv['hardware'] = 'N/A'

    df_ex_ne_inv['bandwidth'] = 'N/A' #la conformacion de este dato requiere desarrollo adicional

    df_ex_ne_inv = _format_reporte(df_ex_ne_inv)

    logging.info ('\n:::Registros existentes en NE y faltan en Inventario: {}'.format(len(df_ex_ne_inv)))

    #_gen_excel(df_ex_ne_inv,'FaltaEnInv')
    df_ex_ne_inv.to_csv('reports/auxiliar/df_ex_ne_inv.csv')

def Caso4_inv_ne(**context):
    manual = """
            Esta funcion determina los registros que existen en el Inventario y NO existen en el NE.
    Args: 
      none
    Returns:
      none
    """
    import pandas as pd
    rol=context['role']

    table_A = 'ne'
    table_B = 'par_inv_itf'

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()

    #existencia:
    #estan en inventario y no estan en NE:
    df_ex_inv_ne = pd.read_sql_query("""select * from {1} where concat IN
                                (
                                select concat from {1} a where networkrole = 'INNER CORE'
                                EXCEPT
                                select concat from {0} b)"""
                                .format(table_A,table_B),con=conn)
    print(len(df_ex_inv_ne))
    
    #df_ex_inv_ne = _format_reporte(df_ex_inv_ne)
    #_gen_excel(df_ex_inv_ne,'FaltaEnNE')
    logging.info ('\n:::Registros existentes en Inventario y faltan en NE: {}'.format(len(df_ex_inv_ne)))

    conn.close()
    df_ex_inv_ne.to_csv('reports/auxiliar/ex_inv_ne.csv')


#########################################################


#tasks
_extrae_bd_inventario = DummyOperator(task_id='Extrae_bd_inventario', retries=1, dag=dag)

_auto_ansible = PythonOperator(
    task_id='ejecuta_ansible', 
    python_callable=call_ansible,
    op_kwargs={
        'connection':'ansible_proxy'
        },
    dag=dag)

_extrae_bd_NE = PythonOperator(
    task_id='trae_archivos', 
    python_callable=scp_files,
    op_kwargs={
        'connection':'ansible_proxy',
        'local_dir':'/usr/local/airflow/Inner/cu1/interfaces/',
        'remote_dir':'/home/u565589/desarrollo/irs_cu/mejoras_cu1/interfaces/'
        },
    dag=dag)

_carga_inv_to_db = PythonOperator(
    task_id='Carga_inv_to_db',
    python_callable=Load_inv,
    op_kwargs={
        'file':'Table-id_722018305.csv',
        'dir':'Inner',
        'role': '*',
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
        #'file':'huawei_IC1.SLO1_interfaces.txt',
        'file':'*',
        'dir':'Inner/cu1/interfaces',
        'role': '*',
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
    python_callable=Caso1_ok_v2,
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

_caso4 = DummyOperator(task_id='Caso4_ExisteInv_NoExisteNE', retries=1, dag=dag)

"""
_caso4 = PythonOperator(
    task_id='Caso4_ExisteInv_NoExisteNE', 
    op_kwargs={    
    'role':'INNER CORE',
    },
    python_callable=Caso4_inv_ne,
    retries=1, dag=dag
    )
"""

_init_reporting = PythonOperator(
    task_id='Init_Reporting',
    op_kwargs={    
    'dir':'reports',
    },
    python_callable=init_report,
    retries=1, dag=dag)

_imprime_reporte = PythonOperator(
    task_id='Genera_Reporte',
    op_kwargs={    
    'dir':'reports',
    },
    python_callable=gen_excel,
    retries=1, dag=dag)

#Secuencia
_extrae_bd_inventario >> _carga_inv_to_db >> _adecuar_naming_inv >> _init_reporting

_auto_ansible >> _extrae_bd_NE >> _carga_ne_to_db >> _adecuar_naming_ne >> _init_reporting

_init_reporting >> _caso1 >> _imprime_reporte

_init_reporting >> _caso2 >> _imprime_reporte

_init_reporting >> _caso3 >> _imprime_reporte

_init_reporting >> _caso4 >> _imprime_reporte