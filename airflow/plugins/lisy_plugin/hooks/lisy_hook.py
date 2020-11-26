from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import Variable

# import external libraries to interact with target system
# ...

import requests
import base64
import sys
import logging

class LisyHook(BaseHook):
    """
    Hook to interact with the API.
    """

    def __init__(self, *args, **kwargs):
        self.__lisy_init()
        
        try: #carga dos conexiones.
            #lisy_token_test para conseguir un token entregado por 'datapower'
            #lisy_api para acceder con el token a la api
            self.conn_token='Lisy_token_test'
            self.conn_api='Lisy_api'
            #self.conn_id=Variable.get('VAR_LISY_LAB')
            connection_token = BaseHook.get_connection(self.conn_token)
            connection_api = BaseHook.get_connection(self.conn_api)

        except:
            #self.conn_id='lisy_default'
            #connection = BaseHook.get_connection(self.conn_id)
            logging.error ('Error en la lectura de los datos de la conexion')


        #Requiere crear objeto 'Connection' en Airflow
        #self.password = connection_token.password
        self.lisy_token_pwd=connection_token.password
        self.lisy_token_usr=connection_token.login
        #self.lisy_conn_port=connection.port
        self.lisy_token_host=connection_token.host
        
        self.lisy_api_host=connection_api.host


    def get_request(self, token, endpoint, tipo, body=None):
        manual: """

        Esta funcion ejecuta el query en Lisy (condis).
        Necesita un token self.token
        
        Args:
            tipo: GET, POST
            endpoint: el endpoint de la api rest de Lisy
        Returns:
        
        """

        """
        MODIFICAR PARA AGREGAR BODY
        import requests

        url = "https://dlcondisdb18:27443/web/condisrest/port/"

        payload="{\r\n    \"identifier\": {\r\n        \"shelfName\": \"IC1.HOR1\",\r\n        \"portInterfaceName\": \"9/0/1\"\r\n    }\r\n}"
        headers = {
        'Authorization': 'Bearer c9371d7466977261cc3fa4ad25f7bf31',
        'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        print(response.text)
 
        {
            "identifier": {
                "shelfName": "IC1.HOR1",
                "portInterfaceName": "9/0/1"
            }
        }
        """

        self.token = token
        
        url = 'https://{0}/proxyrest/lisy/web/condisrest/{1}'.format(self.lisy_api_host,endpoint)
        #print (':::::URL::::',url)
        
        logging.info ("API CONTENT:::{}".format(url))
        print ('========= TOKEN ======', self.token)
        
        if body is None:
            payload = {}
        else:
            payload = body
        
        #print ('EL PAYLOAD QUE LLEGA AL HOOK', payload)

        headers = {
        'Authorization': 'Bearer {0}'.format(self.token),
        'Cookie':'TS016d2cf6=0154ce2499fd2dd02f2c91cebc97efe857db1c7f91b61612598f800f9710a96f951fcca02cdbe66bc12bff1a7f9dc9e62d2d3f0d16'
        }

        try:
            return requests.request(tipo, url, headers=headers, data = payload)
            #return requests.request(tipo, url, headers=headers, data = payload, verify= False)
            #return r.json()
        except:
            logging.error ('ERROR EN LA QUERY::: QUERY INEXISTENTE')

    
    def chk_token(self):
        #este metodo devuelve la estructura full del token, para que se pueda imprimir

        #self.get_token(full=True)
        #url = "https://{0}:{1}/web/condisrest/accessTokens/{2}".format(self.lisy_conn_host, self.lisy_conn_port,self.token)
        #payload  = {}
        #headers= {}
        #logging.info ("API CONTENT:::{}".format(url))
        #return requests.request('GET', url, headers=headers, data = payload, verify= False)
        #return r.json()
        return (self.get_token(full=True)) 
    

    def get_token (self,full=False):

        manual: """

        Esta funcion obtiene el token entregado por Datapower, dadas las credenciales que se recuperan de las conexiones creadas, y leídas en __init__()
        
        Args: 
            full = True | False (default)
            
                True: asigna a self.token el valor del access_token recibido desde datapower, y devuelve la estructura completa de la respuesta de datapower.
                
                False: asigna a self.token el valor del access_token recibido desde datapower.
        Returns:
            Estructura recibida de datapower, si full=True.
        
        """
        
        Auth_b64 = base64.b64encode((self.lisy_token_usr + ':' + self.lisy_token_pwd).encode('ascii'))
        Auth_b64 = 'Basic ' + str(Auth_b64,'utf-8') #convierto a string porque base64 devuelve binario

        url = "https://{0}/openam/oauth2/realms/root/realms/authappext/access_token?grant_type=client_credentials".format(self.lisy_token_host)
        payload = {}
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            #'Authorization': 'Basic TGlzeUZ1bGxDb25zdW1lcjpUZWNvMjAyMF8=',
            'Authorization': Auth_b64,
            'Cookie':
            'TS01f6b324=0154ce24993a13345c3f2c0ede41c6e79fbfd8aac34f61f79203d0250287f8c16a61a5d513a445490eb21302540a7d39eb16d6ac3f'
            }       


        #url = "https://{0}:{1}/web/condisrest/accessTokens".format(self.lisy_conn_host, self.lisy_conn_port)
        #payload = "{\"user\": \"%(user)s\",\"password\": \"%(pwd)s\"}"%{'user': self.lisy_conn_usr, 'pwd': self.lisy_conn_pwd}
        #headers = {'Content-Type': 'application/json'}
        logging.info ("API CONTENT:::{}".format(url))
        #r = requests.request("POST", url, headers=headers, data = payload, verify= False)
        r = requests.request("POST", url, headers=headers, data = payload)
        
        if r.status_code != 200:
            logging.error ('!::: Error en la solicitud del Token')
            logging.error ('Respuesta recibida: {0}'.format(r._content))
            logging.error ('Codigo de respuesta: {0}'.format(r.status_code))
            self.token = '9999999999-GET-ERROR-9999999999'
        else:
            #token = r.json()
            #self.token = token['token']
            self.token = r.json()['access_token']
            if (full): return r.json() #devuelve la estructura full del token

    def __lisy_init(self):
        print ('\n')
        print ('Initializing Lisy API')
        print ('\n')