from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import Variable

# import external libraries to interact with target system
# ...

import requests
import sys
import logging

class LisyHook(BaseHook):
    """
    Hook to interact with the API.
    """

    def __init__(self, *args, **kwargs):
        self.__lisy_init()
        
        try: #check que haya creado la variable & la conexion para Lab
            #conexion a Lab requiere Variable crear en VAR_LISY_LAB=lisy_lab en airflow
            self.conn_id=Variable.get('VAR_LISY_LAB')
            connection = BaseHook.get_connection(self.conn_id)
        except:
            self.conn_id='lisy_default'
            connection = BaseHook.get_connection(self.conn_id)


        #Requiere crear objeto 'Connection' en Airflow
        self.password = connection.password
        self.lisy_conn_pwd=connection.password
        self.lisy_conn_usr=connection.login
        self.lisy_conn_port=connection.port
        self.lisy_conn_host=connection.host


    def get_request(self, operador, tipo):
        self.__get_token()
        #print('Estoy en el display', apiToken)
        #url = "https://dlcondisdb21:27443/web/condisrest/{}/queries/AllShelf_and_clasification_demo.json".format(apiToken)
        url = "https://{0}:{1}/web/condisrest/{2}/{3}".format(self.lisy_conn_host, self.lisy_conn_port, self.token, operador)
        payload  = {}
        headers= {}
        #devuelvo completo, por si en algun caso la respuesta no fuera JSON
        logging.info ("API CONTENT:::{}".format(url))
        try:
            return requests.request(tipo, url, headers=headers, data = payload, verify= False)
            #return r.json()
        except:
            logging.info ('ERROR EN LA QUERY::: QUERY INEXISTENTE')


    def chk_token(self):
        self.__get_token()
        url = "https://{0}:{1}/web/condisrest/accessTokens/{2}".format(self.lisy_conn_host, self.lisy_conn_port,self.token)
        payload  = {}
        headers= {}
        logging.info ("API CONTENT:::{}".format(url))
        return requests.request('GET', url, headers=headers, data = payload, verify= False)
        #return r.json()

    def __get_token(self):
        url = "https://{0}:{1}/web/condisrest/accessTokens".format(self.lisy_conn_host, self.lisy_conn_port)
        #url = "https://ulcondisdb3:27443/web/condisrest/accessTokens"
        #payload = "{\r\n    \"user\": \"u999999\",\r\n    \"password\": \"VGVsZWNvbTEyMw==\"\r\n}"
        payload = "{\"user\": \"%(user)s\",\"password\": \"%(pwd)s\"}"%{'user': self.lisy_conn_usr, 'pwd': self.lisy_conn_pwd}
        headers = {'Content-Type': 'application/json'}
        logging.info ("API CONTENT:::{}".format(url))
        r = requests.request("POST", url, headers=headers, data = payload, verify= False)                
        token = r.json()
        self.token = token['token']


    def __lisy_init(self):
        print ('\n')
        print ('Initializing Lisy API')
        print ('\n')