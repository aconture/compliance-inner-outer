from psycopg2.extras import execute_values
from airflow.hooks import PostgresHook
import pandas as pd
import logging
import os
from influxdb import InfluxDBClient
from airflow.hooks.base_hook import BaseHook

#####################################################################
#####################################################################

def _insert_cursor(dataframe,tabla_postgres, lista_columnas):
    
    """

    Esta funcion inserta registros en la base postgres, usando un cursor.
    
    Args: 
      dataframe [object]: el dataframe que voy a escribir en la base de datos. Tiene que venir un dataframe flat (no tiene que ser multi-index)
      tabla_postgres [text]: la tabla donde voy a escribir los datos
      lista_columnas [list]: la lista de columnas de la tabla que se va a escribir
    
    Returns:
      none

    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()

    sql_string = 'INSERT INTO {} ('.format(tabla_postgres)+ ', '.join(lista_columnas) + ") (VALUES %s)"
    logging.info ('::: Insertando en la tabla: {0}'.format(sql_string))

    values = list(dataframe.itertuples(index=False, name=None))
    logging.info ('::: La siguiente cantidad de registros: {0}'.format(len(values)))

    execute_values(pg_cursor, sql_string, values)
    conn.commit()
    conn.close()

#####################################################################
#####################################################################

def _delete_cursor(sql_string):
    
    """

    Esta funcion borra registros en la base postgres, usando un cursor.
    
    Args: 
      sql_string [text]: la tabla donde voy a escribir los datos
      lista_columnas [list]: la lista de columnas de la tabla que se va a escribir
    
    Returns:
      none

    """

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()

    logging.info ('::: Limpiando registros: {0}\n'.format(sql_string))

    #execute_values(pg_cursor, sql_string, values)
    pg_cursor.execute (sql_string )
    conn.commit()
    conn.close()

#####################################################################
#####################################################################

def Load_inv(**context):
    
    """

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
        #print ('PPPPPPPPPPPPPPPPP',nom_archivo)
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


####################################################################
####################################################################
#Completar función info de errores del ansible##

def insert_ansible_failures(ansibleprintfailures):

    """ Insertar ansibleprintfailures en la tabla ansible_history """

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    conn = pg_hook.get_conn()
    pg_cursor = conn.cursor()

    sql_delete = 'delete from ansible_history'
    _delete_cursor(sql_delete)

    for item in ansibleprintfailures:
        sql = "INSERT INTO ansible_history(ne) VALUES(\'{0}\');".format(item)
        pg_cursor.execute (sql)
    conn.commit()
    conn.close()

####################################################################
####################################################################
#Inserta resultado de ejecucion en influxdb#
####################################################################
####################################################################

def insert_influxdb(data):

    """ Insertar datos en la base de influx, recibe data que es una estructura que contiene
    los tags, los fields y el body. Los parametros de la base influx lo obtiene de las variables
    de conexion de airflow. En particular se llama influxdb_conn """

    try: 
        conn='influxdb_conn'
        influxdb_user=BaseHook.get_connection(conn).login
        influxdb_password=BaseHook.get_connection(conn).password
        influxdb_host=BaseHook.get_connection(conn).host
        influxdb_port=BaseHook.get_connection(conn).port
        influxdb_database=BaseHook.get_connection(conn).schema
    except:
        logging.error ('\n\n:::! Error - Conexion.')
        raise ValueError('Error de recupero de conexion')
    
    print ('influxdb_user: ' + influxdb_user)
    print ('influxdb_password: ' + influxdb_password)
    print ('influxdb_host: ' + influxdb_host)
    print ('influxdb_port: ' + str(influxdb_port))
    print ('influxdb_database: ' + influxdb_database)

    client = InfluxDBClient(host=influxdb_host, port=influxdb_port, username=influxdb_user, password=influxdb_password)
    client.switch_database(influxdb_database)
    client.write_points(data)

####################################################################
####################################################################
####################################################################
####################################################################




