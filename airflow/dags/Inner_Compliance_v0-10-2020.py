from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from airflow.hooks import PostgresHook

from time import sleep
from datetime import datetime, timedelta
import os
import logging


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

#dag
dag = DAG(
    dag_id='Compliance_Inner_Outer-10-2020', 
    schedule_interval= None,
    tags=['inventario', 'compliance'],
    default_args=default_args
)

#####################################################################

def _check_vigencia(**context):
    manual = """
    
    Chequea si el directorio esta actualizado. 

    args:
    **context: necesario para determinar la fecha de ejecucion del flujo.

    return:
    True si la fecha de ejecucion del script es mas actual a la fecha de actualizacion del directorio, eso quiere decir que el directorio se debe actualizar.

    False si la fecha de ejecucion del script es mas vieja a la fecha de actualizacion del directorio, eso quiere decir que el directorio se debe actualizar.
    """        

    date_exec=context['ds'] #ds contiene la fecha de ejecucion del dag yyyy-mm-dd
    modTimesinceEpoc = os.path.getmtime('/usr/local/airflow/Inner/cu1/interfaces') #fecha de ultima ejecucion del ansible
    modificationTime = datetime.fromtimestamp(modTimesinceEpoc).strftime('%Y-%m-%d')
    
    if (date_exec > modificationTime):
        logging.info (':::La informacion no esta vigente, tiene fecha en el pasado: {0}.'.format(modificationTime))
        return (True)

    else:
        logging.info (':::La informacion esta vigente, con fecha actual {0}.\n'.format(modificationTime))
        #return (False)
        return (True)

def call_ansible(**context):
    manual = """
    Ejecutas ansible en el directorio remoto. 
    Si el directorio que contiene el resultado de ansible tiene la fecha actual, no se ejecuta ansible.

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

    update = _check_vigencia (**context)

    if (update):
        logging.info (':::La base de Network Element tiene fecha en el pasado. Ejecutando Ansible para actualizar.')
        try:
            #connection = BaseHook.get_connection(conn_id)
            #host = connection.host
            #user = connection.login
            #passw = connection.password

            #la linea siguiente la comento para no romper produccion
            os.system ('rm /usr/local/ansible/mejoras_cu1/interfaces/*.txt; cd /usr/local/ansible/mejoras_cu1/yaml; ansible-playbook main.yaml')
            #os.system ('sshpass -p {0} ssh {1}@{2} \'rm /home/u565589/desarrollo/irs_cu/mejoras_cu1/interfaces/*.txt; cd /home/u565589/desarrollo/irs_cu/mejoras_cu1/yaml/; ansible-playbook main.yaml\''.format(passw,user,host))
            #os.system ('sshpass -p {0} ssh {1}@{2} \'pwd; ls -lrt\''.format(passw,user,host))
        except:
            logging.error ('\n\n:::! Problema en la conexión al servidor remoto.\n')
            return -1
    else:
        logging.info (':::Base de datos de NE ya actualizada, no es necesario actualizar.')
    
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

    update = _check_vigencia (**context)

    if (update):
        try:
            #connection = BaseHook.get_connection(conn_id)
            #host = connection.host
            #user = connection.login
            #passw = connection.password

            os.system('rm {}*.txt'.format(local_dir))
            logging.info ('::: Inicializado el directorio de *.txt local: {0}'.format(local_dir))
            
            logging.info ('::: Trayendo archivos *.txt del directorio ansible remoto')
            #os.system('sshpass -p {0} scp {1}@{2}:{3}*.txt {4}'.format(passw,user,host,remote_dir,local_dir))
            os.system('cp -p {0}*.txt {1}'.format(remote_dir,local_dir))
            logging.info ('::: Archivos *.txt ansible copiados al directorio local')

        except:
            logging.error (':::! Problema en la conexión al servidor remoto.\n')
            return -1
    else:
        logging.info (':::Base de datos de NE ya actualizada, no es necesario copiar.')

def _insert_cursor(dataframe,tabla_postgres, lista_columnas):
    manual = """
    Esta funcion inserta registros en la base postgres, usando un cursor.
    
    Args: 
      dataframe [object]: el dataframe que voy a escribir en la base de datos. Tiene que venir un dataframe flat (no tiene que ser multi-index)
      tabla_postgres [text]: la tabla donde voy a escribir los datos
      lista_columnas [list]: la lista de columnas de la tabla que se va a escribir
    
    Returns:
      none
    """
    from psycopg2.extras import execute_values
    import pandas as pd

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()

    #columnas = dataframe.columns.ravel()
    #print (columnas)
    sql_string = 'INSERT INTO {} ('.format(tabla_postgres)+ ', '.join(lista_columnas) + ") (VALUES %s)"
    logging.info ('::: Insertando en la tabla: {0}'.format(sql_string))

    values = list(dataframe.itertuples(index=False, name=None))
    logging.info ('::: La siguiente cantidad de registros: {0}'.format(len(values)))

    execute_values(pg_cursor, sql_string, values)

    conn.commit()
    conn.close()

def _delete_cursor(sql_string):
    manual = """
    Esta funcion borra registros en la base postgres, usando un cursor.
    
    Args: 
      sql_string [text]: la tabla donde voy a escribir los datos
      lista_columnas [list]: la lista de columnas de la tabla que se va a escribir
    
    Returns:
      none
    """
    from psycopg2.extras import execute_values
    import pandas as pd

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()

    logging.info ('::: Limpiando registros: {0}\n'.format(sql_string))

    #execute_values(pg_cursor, sql_string, values)
    pg_cursor.execute (sql_string )

    conn.commit()
    conn.close()


def Load_inv(**context):
    manual = """
    Esta funcion carga los registros leidos del archivo indicado en la tarea, en la tabla de postgres indicada en la tarea.
    Previamente, inicializa la tabla indicada borrando todos sus registros.
    La tabla indicada tiene que estar previamente creada en la base de datos de destino.
    
    Args: 
      dir [text]: directorio dentro del home de Airflow, donde se encuentra el archivo o el grupo de archivos a cargar. No debe incluir '/' al final.
      role [text]: rol definido en el inventario. Si su valor = '*', se cargan todos los roles leidos. El 'rol' es una propiedad del inventario.
      file [text]: archivo con la base a cargar. Si su valor = '*', se cargan todos los archivos del directorio indicado.      
      table [text]: tabla de la base de datos que se debe cargar.    
      datatype [text]: tipo de dato que contiene el archivo que se va a importar en la base de datos:
        'json'
        'csv'

    Returns:
      none
    """
    import pandas as pd

    file=context['file']
    print (':::::::::::::::',file)

    try:
        file=context['file']
        dir=context['dir']
        table=context['table']
        rol=context['role']
        datatype=context['datatype']
    except:
        logging.error ('\n:::! Error - Falta un argumento de llamada a esta funcion.')
        logging.info (manual)
        return -1

    if file[0] == '*':
        #cargo en una lista todos los archivos del directorio
        archivos=os.listdir(os.path.join(os.getcwd(),dir))
    else:
        #lista con el archivo que vino como argumento
        archivos = file
    
    #init de la base
    sql_delete = 'DELETE FROM {}'.format(table)
    _delete_cursor(sql_delete)
    logging.info('\n::: Tabla \'{}\' inicializada.'.format(table))

    #var para log:
    file_ok = []
    len_ok = []
    file_nok = []
    len_nok = []
    
    # implementa BD:
    for nom_archivo in archivos:
        abspath = os.path.join(os.getcwd(),dir,nom_archivo)
        print ('PPPPPPPPPPPPPPPPP',nom_archivo)
        try:
            logging.info ('\n::: Iniciando la carga.')
            #los argumentos: warn_bad_lines=True, error_bad_lines=False evitan error de '|' en campo de datos
            #encoding='latin-1' evita el error de "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xXX in position YY: invalid continuation byte" que recibia en algunos archivos dump que importados
            if datatype == 'json':
                df = pd.read_json(abspath,orient='columns')
            if datatype == 'csv':
                df = pd.read_csv(abspath,delimiter='|',
                warn_bad_lines=True, error_bad_lines=False, encoding='latin-1')

            if rol != '*':
                try:
                  df = df[(df['shelfNetworkRole'] == rol)] #filtro el rol de la base traida del inventario
                except:
                  pass
            logging.info ('\n::: Cargando desde Archivo {0}, {1} registros con el rol \'{2}\'.'.format(nom_archivo,len(df), rol))
            
            columnas = df.columns.ravel()
            _insert_cursor(df,table,columnas)

            file_ok.append(abspath)
            len_ok.append(len(df))
            #logging.info ('\n::: Populada tabla \'{}\' con {} registros, tomados de {}.'.format(table,len(values),abspath))
        
        except FileNotFoundError as e:
            logging.error ('\n\n:::! Error - No se encuentra el archivo origen {0}\n'.format(nom_archivo))
            #logging.info('\n{}'.format(manual))
            return
        
        except Exception as e:
            logging.error ('\n\n:::! Error leyendo registros del archivo {0}\n'.format(abspath),exc_info=True)
            file_nok.append(abspath)
            len_nok.append(len(df))

    print ('\n--------------------------------')
    print ('::: Resumen de archivos con errores:')
    for i in range(len(file_nok)):
        pass
        print ('::: Archivo {0} - Registros: {1}'.format(file_nok[i],len_nok[i]))
    
    print ('\n--------------------------------')
    print ('::: Resumen de archivos implementados exitosamente en la tabla \'{}\':'.format(table))
    #print (':::::::archivos ok: ',file_ok)
    for i in range(len(file_ok)):
        print (':::Archivo {0} - Registros: {1}'.format(file_ok[i],len_ok[i]))
        #pass
    print ('\n--------------------------------')


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
    import pandas as pd

    table_ = 'inv_itf'
    table_dest = 'par_inv_itf'

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
    _delete_cursor(sql_delete)
    logging.info('\n::: Tabla {} inicializada.'.format(table_dest))

    #populo la base destino
    columnas = df_inv_itf.columns.ravel()
    _insert_cursor(df_inv_itf,table_dest,columnas)
    

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
    _delete_cursor(sql_delete)
    logging.info('\n::: Tabla {} inicializada.'.format(table_dest))

    #populo la base destino
    columnas = df_ne.columns.ravel()
    _insert_cursor(df_ne,table_dest,columnas)
    

def gen_excel(**context):
    manual = """
    Esta funcion 
        -genera un archivo excel con el contenido de los 'n' archivos csv que lee en el directorio. Requiere que todos los archivos csv tengan los mismos campos.
        Para que no repita el nombre de la solapa del excel, se debe invocar a la funcion init_report, que borra el excel que existe previamente.

        -graba el resumen del resultado en una tabla historica en la base de datos.
    
    Args: 
      none
    Returns:
      none
    """

    import openpyxl
    import pandas as pd

    dir=context['dir']
    #archivos=os.listdir(os.path.join(os.getcwd(),dir,'auxiliar'))
    archivos = [f for f in os.listdir(os.path.join(os.getcwd(),dir,'auxiliar')) if f.endswith('.csv')]
    
    dataframe = pd.DataFrame()
    for nom_archivo in archivos:
        abspath = os.path.join(os.getcwd(),dir,'auxiliar',nom_archivo)
        dataframe_aux = pd.read_csv(abspath,delimiter=',')
        dataframe = pd.concat ([dataframe,dataframe_aux],sort=False) #sort=False para evitar un warning de Pandas
    
    # Generacion de la solapa Resumen
    data_resumen = dataframe.pivot_table(
		index=['NE'],
		columns='EvEstado',
		aggfunc={'EvEstado':'count'},
		fill_value=0
	)    
    data_resumen.sort_values(
        by=['NE'],
        inplace=True,
        ascending=False
    )

    f_ejecucion=context['ds']

    #Convierto el pivot en dataframe y lo guardo en html para usarlo en el mail a enviar
    data_resumen_dataframe = data_resumen.reset_index()
    data_resumen_dataframe['fecha'] = f_ejecucion

    data_resumen_dataframe.to_html('reports/auxiliar/resumen.html', index=False)

    #Guardo los registos del pivot en una tabla que contiene el historico:
    tabla = 'core_history'
    #elimino para evitar registros duplicados de la misma fecha:
    sql_delete = 'delete from core_history where fecha=\'{0}\''.format(f_ejecucion)
    _delete_cursor(sql_delete)
    #registro el resumen en la tabla historico de la base de datos:
    lista_columnas = ['NE', 'ok', 'revisar', 'finv', 'fecha']
    _insert_cursor (data_resumen_dataframe, tabla, lista_columnas)

    #print (dataframe)
    archivo_rep = os.path.join(os.getcwd(),dir,'reporte.xlsx')        
    try:
        with pd.ExcelWriter(archivo_rep,mode='a',engine='openpyxl', encoding="utf-8-sig") as escritor:
            data_resumen.to_excel(escritor, sheet_name='resumen')
            dataframe.to_excel(escritor, sheet_name='crudo', index=False)
    except FileNotFoundError:
        with pd.ExcelWriter(archivo_rep,mode='n',engine='openpyxl', encoding="utf-8-sig") as escritor:
            data_resumen.to_excel(escritor, sheet_name='resumen')
            dataframe.to_excel(escritor, sheet_name='crudo', index=False)
    finally:
        escritor.save

def _cuerpo_mail():
    """
    Lee el resumen generado y almacenado en reports/auxiliar/ para usarlo en el cuerpo del mail de resultado de la operacion.
    """
    with open('reports/auxiliar/resumen.html', 'r') as f:
        html_string = f.read()
    f.close
    return (html_string)

def init_report(**context):
    import os
    dir=context['dir']
    try:
        os.remove('reports/reporte.xlsx')
    except FileNotFoundError:
        pass

def _format_reporte_compliance(dataframe):
    dataframe = dataframe.rename(columns={
      'portoperationalstate_x':'EstadoRed',
      'portoperationalstate_y': 'EstadoLisy',
      'info1':'DescRed',
      'portinfo1':'DescLisy',
      'shelfname_x':'NE',
      'concat':'Recurso',
      'shelfhardware':'shelfHardware', #en la bd de postgres esta en minuscula, y en el dump tiene una mayuscula
      'shelfnetworkrole':'shelfNetworkRole', #en la bd de postgres esta en minuscula, y en el dump tiene una mayuscula
      'portbandwidth':'portBandwidth'
      })
    
    print ('En el formateo:',dataframe.columns)

    dataframe = dataframe [[
      'shelfNetworkRole', #tomado de Lisy
      'NE', #tomado del NE
      'shelfHardware', #tomado de Lisy
      'interface',
      'Recurso',
      'portBandwidth',
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
    df_ok = _format_reporte_compliance(df_ok)

    #print (df_ok.columns.ravel())

    conn.close()
    
    logging.info ('\n:::Registros ok: {}'.format(len(df_ok)))

    #impresiones:
    print (len(df_ok))

    df_ok.to_csv('reports/auxiliar/ok.csv', index=False)
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
    print (df_rev.columns)
    df_rev = _format_reporte_compliance(df_rev)

    conn.close()

    logging.info ('\n:::Registros a revisar: {}'.format(len(df_rev)))

    #_gen_excel(df_rev,'revisar')

    #print (len(df_rev))
    df_rev.to_csv('reports/auxiliar/rev.csv', index=False)

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

    df_ex_ne_inv = _format_reporte_compliance(df_ex_ne_inv)

    logging.info ('\n:::Registros existentes en NE y faltan en Inventario: {}'.format(len(df_ex_ne_inv)))

    #_gen_excel(df_ex_ne_inv,'FaltaEnInv')
    df_ex_ne_inv.to_csv('reports/auxiliar/df_ex_ne_inv.csv', index=False)

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
                                select concat from {1} a where shelfnetworkrole = 'INNER CORE'
                                EXCEPT
                                select concat from {0} b)"""
                                .format(table_A,table_B),con=conn)
    print(len(df_ex_inv_ne))
    
    logging.info ('\n:::Registros existentes en Inventario y faltan en NE: {}'.format(len(df_ex_inv_ne)))

    conn.close()
    df_ex_inv_ne.to_csv('reports/auxiliar/ex_inv_ne.csv', index=False)


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
        'remote_dir':'/usr/local/ansible/mejoras_cu1/interfaces/'
        },
    dag=dag)

_carga_inv_to_db = PythonOperator(
    task_id='Carga_inv_to_db',
    python_callable=Load_inv,
    op_kwargs={
        #'file':'Table-id_722018305.csv',
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
        #'file':'huawei_IC1.HOR1_interfaces.txt',
        #'file':'huawei_IC1.SLO1_interfaces.txt',
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

_envia_mail1 = EmailOperator(
    task_id='Email_to_canal',
    #to="agconture@teco.com.ar",
    #to="mbaletti@fibertel.com.ar",
    to="c23383e8.teco.com.ar@amer.teams.ms", #canal teams de in-house
    #to="b70919fe.teco.com.ar@amer.teams.ms", #mail del canal de compliance
    subject="Compliance Inner&Outer - Resultado de Ejecucion {{ ds }}",
    #html_content="<h3> Esto es una prueba del envio de mail al finalizar la ejecucion del pipe </h3>",
    html_content=_cuerpo_mail(),
    files=["/usr/local/airflow/reports/reporte.xlsx"],
    dag=dag
)

#Secuencia
_extrae_bd_inventario >> _carga_inv_to_db >> _adecuar_naming_inv >> _init_reporting

_auto_ansible >> _extrae_bd_NE >> _carga_ne_to_db >> _adecuar_naming_ne >> _init_reporting

_init_reporting >> _caso1 >> _imprime_reporte

_init_reporting >> _caso2 >> _imprime_reporte

_init_reporting >> _caso3 >> _imprime_reporte

_init_reporting >> _caso4 >> _imprime_reporte

_imprime_reporte >> _envia_mail1
