from airflow.hooks.base_hook import BaseHook
from airflow.hooks import PostgresHook
import pandas as pd
import logging
from time import sleep
from datetime import datetime, timedelta
import os
import ansible_runner


#####################################################################
#####################################################################

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
            os.system('rm {}*.txt'.format(local_dir)) #Limpiamos directorio
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

#####################################################################
#####################################################################

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
            #os.system ('rm /usr/local/ansible/mejoras_cu1/interfaces/*.txt; cd /usr/local/ansible/mejoras_cu1/yaml; ansible-playbook main.yaml')
            
            r = ansible_runner.run(private_data_dir='/usr/local/ansible/mejoras_cu1/yaml', playbook='main.yaml')
            print("{}: {}".format(r.status, r.rc))
            # successful: 0
            for each_host_event in r.events:
                print(each_host_event['event'])
            print("Final status:")
            print(r.stats)  

        except:
            logging.error ('\n\n:::! Problema en la conexión al servidor remoto.\n')
            print(r.stats)
            return -1
    else:
        logging.info (':::Base de datos de NE ya actualizada, no es necesario actualizar.')


#####################################################################
#####################################################################

def _check_vigencia(**context):
    
    """
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