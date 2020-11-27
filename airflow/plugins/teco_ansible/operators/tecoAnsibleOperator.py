from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from lib.teco_db import *
import logging
import ansible_runner
import os


class tecoCallAnsible(BaseOperator):
    """
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

    @apply_defaults
    def __init__(
        self, 
        *args, 
        **context):
        super(tecoCallAnsible, self).__init__(*args, **context)
        
        try: #parametros obligatorios
            self.pbook_dir = context['op_kwargs']['pbook_dir']
            self.playbook = context['op_kwargs']['playbook']
            self.envvars = dict(ansible_user=context['op_kwargs']['user'], ansible_password=context['op_kwargs']['pass'])
        except:
            logging.error ('\n\n:::! Error - Falta un argumento de llamada a esta funcion.')
            #logging.error (manual)

        try: #parametros opcionales
            self.init_output = context['op_kwargs']['init_output']
            self.mock_source = context['op_kwargs']['mock_source']
            self.mock_dest = context['op_kwargs']['mock_dest']
            self.init_output = context['op_kwargs']['init_output']
            self.inventory = context['op_kwargs']['inventory']
        except:
            pass

        try:
            self.mock = context['op_kwargs']['mock']
        except:
            self.mock = True
        

    def execute(self, **context):
        if self.mock:
            try:
                os.system ('rm {0}'.format(self.init_output))
                os.system ('cp -p {0} {1}'.format(self.mock_source, self.mock_dest))
                logging.info ('\n\n:::Salimos por mockeo...\n\n')

            except:
                logging.error ('Error al recuperar o ejecutar un parametro de mockeo. Ver manual:\n')
                raise ValueError ('Error en mockeo')

            finally:
                return

        self.update = self._check_vigencia()
        if (self.update):
            logging.info (':::La base de Network Element tiene fecha en el pasado. Ejecutando Ansible para actualizar.')
            try:
                try:
                    os.system ('rm {0}'.format(self.init_output))
                except:
                    pass

                try:
                    r = ansible_runner.run(private_data_dir=self.pbook_dir, playbook=self.playbook, inventory=self.inventory, extravars=self.envvars)
                    print(self.envvars)
                except:
                    r = ansible_runner.run(private_data_dir=self.pbook_dir, playbook=self.playbook)
                print("{}: {}".format(r.status, r.rc))
                # successful: 0
                for each_host_event in r.events:
                    print(each_host_event['event'])
                print("Final status:")
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print (':::::::::::::::::::::::::SALIDA:::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print("La salida de ansible es: ",r.stats)

                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                
            except:
                logging.error ('\n\n:::! Problema en la conexión a la Red.\n')
                raise ValueError ('Error en la conexión a la Red')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print ('::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::')
                print("La salida de ansible es: ",r.stats)  
                return -1

            finally:
                ansibleprint_raw_processed = r.stats["processed"]
                ansibleprint_raw_failures = r.stats["failures"]
                print ('Los procesados correctamente son: ', ansibleprint_raw_processed)
                print ('Las fallas de ejecución son las siguientes: ', ansibleprint_raw_failures)               

                LansibleFairlure = []

                if ansibleprint_raw_processed is None:
                    logging.error ('\n\n:::! Por favor revisar inventario y conectividad de red !:::.\n')
                    raise ValueError ('Error en la conexión a la Red')
                else:
                    for fairlureItem in ansibleprint_raw_failures:
                        LansibleFairlure.append (fairlureItem)
                    logging.info (':::Elementos fallados {0}'.format(LansibleFairlure))
                    insert_ansible_failures(LansibleFairlure)

        else:
            logging.info (':::Base de datos de NE ya actualizada, no es necesario actualizar.')


    def _check_vigencia(self):
        
        """
        
        Chequea si el directorio esta actualizado. 

        args:
        **context: necesario para determinar la fecha de ejecucion del flujo.

        return:
        True si la fecha de ejecucion del script es mas actual a la fecha de actualizacion del directorio, eso quiere decir que el directorio se debe actualizar.

        False si la fecha de ejecucion del script es mas vieja a la fecha de actualizacion del directorio, eso quiere decir que el directorio se debe actualizar.
        
        """        
        
        return True

