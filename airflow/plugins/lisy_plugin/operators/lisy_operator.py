from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

#my hook
from lisy_plugin.hooks.lisy_hook import LisyHook

import logging
import pprint
from pprint import pformat
#import os
import json
#from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

###########################################################################
###########################################################################
class LisyQueryCorporateService(BaseOperator):
    """
    Este operador navega el arbol de inventario a partir del ID DE SERVICIO.

        Args: 
            servid [Text]: id de la Referencia
            params [diccionario]: Set de Parametros opcionales. Puede ser diccionario vacio.

        Returns:
            Escribe una estructura en json, con la siguiente forma:

            {
                "ServiceName": "439341-1",
                "ServiceState": "Active",
                "Userlabel": "439341",
                "UsedPort": {
                    "0/4/0/1": {
                        "InferfaceName": "0/4/0/1",
                        "OperationalState": "Active",
                        "IpAddress": "10.0.1.185"
                        "Vlan": "1003"
                    },
                    "0/7/0/19": {
                        "InferfaceName": "0/7/0/19",
                        "OperationalState": "Active",
                        "IpAddress": "10.0.1.182"
                        "Vlan": "1002"
                    },
                },
            }

    """

    @apply_defaults
    def __init__(
        self, 
        params,
        *args, 
        **context):
        super(LisyQueryCorporateService, self).__init__(*args, **context)
        self.hook = None
        self.params = params
        self.servid = context['servid']
        self.dest_dir = context['dest_dir']
        self.endpoint = 'corporateService/{0}'.format(self.servid)

    ###########################################################################
    def execute(self, **context):
        """
        Ejecuto el query para obtener datos tales como vlan e ip a partir de un id de servicio

        #####
        #ITERACION con el endpoint /corporateService
        #Service    ->  usedPortNames   -->vlanNames
        #                               -->ipAddressNames
        #                               --> etc...
        #####

        """
        
        vista,token = _endpoint_handler(self.hook, self.endpoint)

        print ('\n\n\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')        
        #print ()
        logging.info ('Respuesta recibida desde Lisy con los datos del servicio:\n {0}'.format(pformat(vista)))

        #comienzo a pedir las iteraciones:
        usedPortNames = self._iter_oops(vista, 'usedPortNames', token)
        serviceState = self._iter_oops(vista, 'state', token)
        
        if len(usedPortNames) > 0: #si es cero es porque no tiene ninguna interface asignada
            Port={}
            for item in usedPortNames:
                #logging.info ('\nIterando el listado de recursos del usedPortNames encontrado',item)
                vlanNames = self._iter_oops(item, 'vlanNames', token)
                ipAddressNames = self._iter_oops( item, 'ipAddressNames', token)
                operationalState = self._iter_oops( item, 'operationalState', token)

                logging.info ('\n\n======================')
                #logging.info ('Recursos encontrados para el servicio {0}'.format(self.servid))
                
                #si es un objeto no asignado no va a traer datos tales como Vlan, por lo que si no capturo try/except dará error:
                try:
                    PortBody = {
                        "Vlan":vlanNames[0]['name'], #asumo que 1 usedport solo tendrá 1 vlan
                        "IpAddress":ipAddressNames[0]['name'], #asumo que 1 usedport solo tendrá 1 vlan
                        "InferfaceName":item['userLabel'],
                        "operationalState":operationalState[0]['userLabel']
                    }
                    Port[item['userLabel']]=PortBody
                except:
                    pass

                """
                for elem in vlanNames:
                    logging.info ('La VLAN es:::{0}'.format(elem['name']))
                    vlan = "Vlan":elem['name']
                    #Port[item['userLabel']]={"Vlan":elem['name']} #aca se asignará el valor de la vlan
                for elem in ipAddressNames:
                    logging.info ('La IP es:::{0}'.format(elem['name']))
                    PortBody = PortBody + {"IpAddress":elem['name']}
                    #Port[item['userLabel']]={"IpAddress":elem['name']} #aca se asignará el valor de la IP => casi funciona no descartar
                logging.info ('\n\n======================')
                """
        else:
            logging.info (':::El servicio no tiene interfaces asignadas')

        #Armado del json:
        resultado={}                
        resultado["idServicio"]=vista['name']
        resultado["userLabel"]=vista['userLabel']
        resultado["info1"]=vista['info1']
        resultado["ServiceState"]=serviceState[0]['userLabel']
        resultado["UsedPort"]=Port
        #print ('0000000000000000000000000000000000000',vista['_self']['class'])
        resultado["tipoServicio"]=vista['_self']['class']
                
        logging.info ('\n\n:::Datos obtenidos:\n{0}'.format(pformat(resultado)))

        _to_jsonFile(self.dest_dir, resultado, 'corporateService', self.servid)


    ###########################################################################
    def _iter_oops(self,vista,objeto, token):
        """
        Esta función interactua con el árbol de cada recurso del inventario.
        Itera la 'vista' recibida buscando el 'objeto' indicado.
        
            vista: la estructura json que devolvio la respuesta desde Lisy.
            
            objeto: el objeto a buscar. 
                Ej: usedPortNames
        
            token: el token recibido desde Datapower, para ejecutar los comandos de iterar.
        
        Return:
            devuelve una lista con el resultado de las estructuras encontradas
        """
        
        #instancio el hook porque voy a hacer consultas a la API:
        if not self.hook:
            self.hook = LisyHook()

        #if objeto=='operationalState':logging.info ('\n\nIniciando iteracion de la estructura {0}'.format(pformat(vista)))    

        #logging.info ('\n\nIniciando iteracion de la estructura {0}'.format(pformat(vista)))
        print ('\n\n')
        logging.info ('///////////////////////////////////////////////////////////////')
        logging.info ('::::::===> Entro en la iteracion buscando los {0}<===\n'.format(objeto))
        
        # ITERO BUSCANDO LOS recursos asignados al objeto indicado. Si no tiene ningun recurso, no va a encontrar ningun oop:
        List_items=[] #itero usedPortNames para traerme los port asociados a este servicio
        try:
            for idx in range (0, len(vista[objeto])): #itero la lista de diccionarios
                #print('Valor del OOP para consultar el port es: {}'.format(vista['usedPortNames'][idx]['oop']))
                List_items.append (vista[objeto][idx]['oop'])
        except:
            logging.info ('El objeto {0} no es una lista o no se encuentra asignado, intentando otra iteracion'.format(objeto))
            try:#si se trata de un objeto que no está asignado, da error el append, por eso el try/except
                List_items.append (vista[objeto]['oop'])
            except:
                pass

        List_resultado=[] #cuando x ej tiene asignado mas de 1 'usedPortNames', me devuelve un json por cada 'usedPortNames' asignado al recurso. En esta lista guardo cada uno de los json que recibo, para devolverlos a la funcion que llamo a _iter_oop.
        for item in List_items:
            query = 'oops/{}'.format(item)
            trae = self.hook.get_request(token, query, 'GET')
            try:
                #'vista' es un diccionario con los objetos cargados en el inventario:
                vista = trae.json()
                ###pprint.pprint (vista)
                List_resultado.append (vista)
                print ('\n\n')
            except:
                #ciertos objetos da error y viene en formato texto el error
                vista = trae.text.encode('utf8')
                #return
        #return (List_resultado)
        logging.info ('\nSe encontraron {0} instancias de {1} en la estructura recorrida'.format(len(List_items),objeto))
        logging.debug ('estructura encontrada en la iteraccion de {0} \n\n'.format(objeto))
        logging.debug (pformat(List_resultado))
        logging.info ('saliendo de la iteracion. Se Buscaron estructuras dependientes de {0}'.format(objeto))
        logging.info ('///////////////////////////////////////////////////////////////')
        return List_resultado
        ####
        #aca iterar sobre el resultado para buscar los siguientes oop ====>
        ####

        logging.info ('::::::===> Salgo de la iteracion de {0}<==='.format(objeto))

###########################################################################
###########################################################################

class LisyQueryCustom(BaseOperator):
    """
    Este operador accede a las queries custom.
    Se usa solo en ambiente de TEST, no accede a otros ambientes.
    Este acceso lo maneja Datapower mediante las credenciales de acceso a su API.

        Args: 
            query_id: id de la query
            params [diccionario]: Set de Parametros opcionales. Puede ser diccionario vacio.

        Returns:
            Escribe la estructura en json que devuelve la custom query.

    """
    @apply_defaults
    def __init__(
        self, 
        params,
        *args, 
        **context):
        super(LisyQueryCustom, self).__init__(*args, **context)
        self.hook = None
        self.params = params
        self.query_id = context['query_id']
        self.dest_dir = context['dest_dir']
        self.endpoint = 'queries/{0}.json'.format(self.query_id)

    def execute(self, **context):
       
        vista,token = _endpoint_handler(self.hook, self.endpoint)

        print ('\n\n\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')        
        logging.info ('Respuesta recibida desde Lisy:\n {0}'.format(pformat(vista)))
        
        _to_jsonFile(self.dest_dir, vista, 'customQuery', self.query_id)

###########################################################################
###########################################################################

class LisyQueryPort(BaseOperator):
    """
    Devuelve la informacion de los recursos asociados a un port.

        Args: 
            Shelfname [Text]: id del shelf
                Ej: IC1.HOR1
            Port [Text]: 
                Ej: '9/0/1'
            dest_dir [Text]:
                Directorio donde se almacena el archivo json con el resultado de la consulta.
                Ej: '/usr/local/airflow/reports/Lisy/'

        Returns:
            Escribe la estructura en json que devuelve la consulta.

    """
    @apply_defaults
    def __init__(
        self, 
        params,
        *args, 
        **context):
        super(LisyQueryPort, self).__init__(*args, **context)
        self.hook = None
        self.params = params
        self.port_id = context['port_id']
        self.shelf_name = context['shelf_name']        
        self.dest_dir = context['dest_dir']
        self.endpoint = 'port/'

    def execute(self, **context):
       
        #armo el body:
        #body="{\r\n    \"identifier\": {\r\n        \"shelfName\": \"IC1.HOR1\",\r\n        \"portInterfaceName\": \"9/0/1\"\r\n    }\r\n}"

        body =f'{{\r\n \"identifier\": {{\r\n \"shelfName\": \"{self.shelf_name}\",\r\n \"portInterfaceName\":\"{self.port_id}\" \r\n }} \r\n}}'
        
        #logging.info ('::::::::::::::::{0}'.format(body))

        vista,token = _endpoint_handler(self.hook, self.endpoint, 'POST', body)

        print ('\n\n\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')        
        logging.info ('Respuesta recibida desde Lisy:\n {0}'.format(pformat(vista)))

        fileid = self.shelf_name + '_' + self.port_id
        _to_jsonFile(self.dest_dir, vista, 'QueryPort', fileid)

###########################################################################
###########################################################################

class LisyQueryVlan(BaseOperator):
    """
    Devuelve la informacion de los recursos asociados a una VLAN.

        Args: 
            pool_name [Text] = 
                Nombre del Pool de donde depende la VLAN. En caso que no sea único, el servicio devolverá el primero que encuentre con esos atributos.
                Ej: 'ALF2MU'
            
            type [Text] = 'SVlan' | 'CVLan'. Parametro CASE SENSITIVE.
                Tipo de vlan. 
                Ej: 'SVlan'
            
            name [Text] = 
                Nombre la de VLan.
                Ej: '3990'
            
            dest_dir [Text]:
                Directorio donde se almacena el archivo json con el resultado de la consulta.
                Ej: '/usr/local/airflow/reports/Lisy/'

        Returns:
            Escribe la estructura en json que devuelve la consulta.

    """
    @apply_defaults
    def __init__(
        self, 
        params,
        *args, 
        **context):
        super(LisyQueryVlan, self).__init__(*args, **context)
        self.hook = None
        self.params = params
        self.pool_name = context['pool_name']
        self.type = context['type']        
        self.name = context['name']
        self.dest_dir = context['dest_dir']
        self.endpoint = 'vlanbase/'

    def execute(self, **context):
       
        #armo el body:

        #payload="{\r\n\"identifier\": {\r\n   \"poolName\": \"ALF2MU\",\r\n   \"type\": \"SVlan\",\r\n   \"name\": \"3990\"\r\n   }\r\n}"
        body =f'{{\r\n \"identifier\": {{\r\n \"poolName\": \"{self.pool_name}\",\r\n \"type\":\"{self.type}\", \r\n \"name\": \" {self.name}\"\r\n }} \r\n}}'
        
        #logging.info ('::::::::::::::::{0}'.format(body))

        vista,token = _endpoint_handler(self.hook, self.endpoint, 'POST', body)

        print ('\n\n\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')        
        logging.info ('Respuesta recibida desde Lisy:\n {0}'.format(pformat(vista)))

        fileid = self.pool_name + '_' + self.type + '_' + self.name
        _to_jsonFile(self.dest_dir, vista, 'QueryVlan', fileid)

###########################################################################
###########################################################################

class LisyCheckTokenOperator(BaseOperator):
    """
    This operator checks token health on LISY API-Rest

        Args: 
            <Args de BaseOperator>
            none

        kwarg** Args:
            none

        Returns:
            none
    """

    @apply_defaults
    def __init__(
        self, 
        params,
        *args, 
        **context):
        super(LisyCheckTokenOperator, self).__init__(*args, **context)
        self.hook = None
        
    def execute(self, **context):
        """
        Ejecuto el pedido del token para imprimirlo en pantalla
        """
        if not self.hook:
            #self.hook = LisyHook(location=self.location)
            self.hook = LisyHook()
        trae = self.hook.chk_token() #el chk_token del hook devolvera un diccionario con la estructura completa del token recibido
        print ('\n\n')
        logging.info (pformat(trae))
        print ('\n\n')


###########################################################################
###########################################################################
#
# Funciones comunes a todos los operadores
#

def _to_jsonFile(dest_dir, struct, endpoint, fileid):
    """
    Guarda la estructura en un archivo json en el directorio seleccionado

        dest_dir: 
            directorio de destino donde se va a almacenar el archivo json.

        struct: 
            estructura json que se escribe en el archivo de destino.
        
        endpoint: 
            prefijo que se pone al archivo. Normalmente el nombre del endpoint que invoca el operador.
        
        fileid: 
            el id del nombre del archivo json que se guarda en dest_dir.

    """
    #file_name = self.file_name
    #tot_name = os.path.join(file_name)
    
    #reemplazos de caracteres no validos para nombrar archivos:
    fileid = fileid.replace('/','-')
    fileid = fileid.replace('\\','-')

    #escribo
    file_url = dest_dir + endpoint + '_' + fileid + '.json'
    with open(file_url, 'w') as outputfile:
        json.dump(struct, outputfile)
    logging.info('::: Base almacenada en {}'.format(file_url))
    #pprint.pprint(struct)


def _endpoint_handler(hook, endpoint, metodo='GET', body=None):
    """
    Llama al hook, ejecuta el endpoint recibido, y devuelve el resultado.

        endpoint:
            abc/xyz. Donde abc es el endpoint a ejecutar y xyz los parametros a pasar al endpoint.

        returns:
            vista:
                es la estructura que recibida como respuesta desde la API
            
            token:
                el token recibido desde la API, lo devuelvo para re-utilizarlo en el resto de las consultas de la misma transaccion, cuand hay transacciones anidadas.
    """
    if not hook:
        hook = LisyHook()

    #solicitud del token
    token = hook.get_token(full=True)
    #logging.info (token['access_token'])
    token = token['access_token']

    trae = hook.get_request(token, endpoint, metodo, body)

    try:
        #'vista' es un diccionario con los objetos cargados en el inventario:
        vista = trae.json()

    except:
        #ciertos objetos da error y viene en formato texto el error. En estos casos salgo de la consulta.
        vista = trae.text.encode('utf8')
        logging.error ('El objeto no existe, abortando consulta. Recibido:\n {0}'.format (vista))
        raise ValueError ('\nAbortando consulta por: {0}'.format (vista))
        return

    #si el servicio buscado en la tarea no existe, la API devuelve un json con un status=404
    #si existe el servicio, la API devuelve un json, y no envía la clave status
    try:
        if vista['status'] == 404:
            logging.error ('El servicio solicitado no existe {0}'.format(self.servid))
            raise ValueError ('Servicio {0} no encontrado '.format(self.servid))
            return
    except:
        pass


    return (vista, token)