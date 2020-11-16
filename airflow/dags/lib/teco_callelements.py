from airflow.hooks.base_hook import BaseHook
from airflow.hooks import PostgresHook
import pandas as pd
import logging
from time import sleep
from datetime import datetime, timedelta
import os
import ansible_runner
import json
from lib.teco_db import *


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

    Ejecuta ansible en el directorio remoto. 
    Si el directorio que contiene el resultado de ansible tiene la fecha actual, no se ejecuta ansible.

    Args: 
        pbook_dir: [Text]
            El directorio donde esta ubicado el plabook a ejecutar.

        playbook: [Text]
            El nombre del playbook a ejecutar.
        
        [inventory]: [text]
            Inventario a utilizar.
            Sobreescribe el inventario configurado en el script de ansible.

        [init_output]: [text]
            El path que se necesita limpiar antes de la ejecucion del playbook. Soporta wildcard (ej: *.txt).

        [mock]: [True (default) | False]
            Evita ejecutar el playbook de ansible, usando datos mockeados.
                True: usa información de NEs mockeada.
                False: funcionamiento normal: ejecuta ansible para consultar la info en los NE

        [mock_source]: [Text]
            La ubicación de los archivos que contienen la información para realizar el mock. 
            Soporta wildcard (ej: *.txt)

        [mock_dest]: [Text]
            El directorio de destino de los archivos que contienen la información para realizar el mock. 
            Es el directorio de trabajo de donde toma la información el caso de uso.

    """
    try:
        pbook_dir = context['pbook_dir']
        playbook = context['playbook']
    except:
        logging.error ('\n\n:::! Error - Falta un argumento de llamada a esta funcion.')
        logging.error (manual)
        return -1

    try:
        mock = context['mock']
    except:
        mock = True

    if mock:
        #os.system ('rm /usr/local/ansible/mejoras_cu1/interfaces/*.txt; cp -p /usr/local/ansible/mejoras_cu1/interfaces_mock/*.txt /usr/local/ansible/mejoras_cu1/interfaces/')
        try:
            init_output = context['init_output']
            mock_source = context['mock_source']
            mock_dest = context['mock_dest']
            os.system ('rm {0}'.format(init_output))
            os.system ('cp -p {0} {1}'.format(mock_source, mock_dest))
            logging.info ('\n\n:::Salimos por mockeo...\n\n')
        except:
            logging.error ('Error al recuperar o ejecutar un parametro de mockeo. Ver manual:\n {0}'.format(manual))
            raise ValueError ('Error en mockeo')
        #os.system ('cp -p /usr/local/ansible/mejoras_cu1/interfaces_mock/*.txt /usr/local/ansible/mejoras_cu1/interfaces/')

        return

    update = _check_vigencia (**context)
    if (update):
        logging.info (':::La base de Network Element tiene fecha en el pasado. Ejecutando Ansible para actualizar.')
        try:
            #os.system ('rm /usr/local/ansible/mejoras_cu1/interfaces/*.txt; cd /usr/local/ansible/mejoras_cu1/yaml; ansible-playbook main.yaml')
            try:
                init_output = context['init_output']
                os.system ('rm {0}'.format(init_output))
            except:
                pass

            try:
                inventory = context['inventory']
                r = ansible_runner.run(private_data_dir=pbook_dir, playbook=playbook, inventory=inventory)
            except:
                r = ansible_runner.run(private_data_dir=pbook_dir, playbook=playbook)

            print("{}: {}".format(r.status, r.rc))
            # successful: 0
            for each_host_event in r.events:
                print(each_host_event['event'])
            print("Final status:")
            print("====================================")
            print("====================================")
            print("La salida de ansible es: ",r.stats)

            print("====================================")
            print("====================================")
            print("====================================")
            print("====================================")

            # if len(LansibleFairlure)==0:
            #     logging.info ('\n\n:::! Ansible ejecutado correctamente para todos los elementos de red\n')

            # LansibleFairlure=[]
            # if ansibleprintfailures is None:
            #     logging.info ('\n\n:::! Ansible ejecutado correctamente para todos los elementos de red\n')
            # else:
            #     for ansibleprintfailures in ansibleprint_raw:
            #         print ("Las fallas de ejecución fueron las siguientes: ",ansibleprintfailures)
            #         #FUNCION BORRADO
            #         #ansiblefailures = teco_db.insert_ansible_failures(ansibleprintfailures)
            #         #print ("Salida insert DB: ",ansiblefailures)
            #         LansibleFairlure.append (ansibleprintfailures)

            # logging.info (':::Elementos fallados'.format(LansibleFairlure))


        except:
            logging.error ('\n\n:::! Problema en la conexión a la Red.\n')
            raise ValueError ('Error en la conexión a la Red')
            print("====================================")
            print("====================================")
            print("La salida de ansible es: ",r.stats)  
            return -1

        finally:
            ansibleprint_raw = r.stats["failures"]
            print ('ESTOY EJECUTANDO ANSIBLE::::::::::::::::::::::::::')
            print ('EL ansibleprint_raw es ', ansibleprint_raw)

            LansibleFairlure = []
            for fairlureItem in ansibleprint_raw:
                print (':::::::::::::::::::',fairlureItem)
                LansibleFairlure.append (fairlureItem)
            logging.info (':::Elementos fallados {0}'.format(LansibleFairlure))
            insert_ansible_failures(LansibleFairlure)

    else:
        logging.info (':::Base de datos de NE ya actualizada, no es necesario actualizar.')


#####################################################################
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