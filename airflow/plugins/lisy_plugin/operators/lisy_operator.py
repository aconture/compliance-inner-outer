from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

#my hook
from lisy_plugin.hooks.lisy_hook import LisyHook

import logging
import pprint
import os
import json
#from datetime import datetime

class LisyQueryOperator(BaseOperator):
    """
    This operator queries on LISY API-Rest

        Args: 
            <Args de BaseOperator>
            params [diccionario]: Set de Parametros opcionales. Puede ser diccionario vacio.
            metodo [text]: El query a ejecutar, existente en Lisy.

        kwarg** Args:
            objeto [text]: Un objeto a buscar en Lisy.
        Returns:
            none
    """

    @apply_defaults
    def __init__(
        self, 
        params,
        metodo,
        *args, 
        **kwargs):
        super(LisyQueryOperator, self).__init__(*args, **kwargs)
        self.hook = None
        self.metodo = metodo
        self.params = params
        self.objeto = kwargs['objeto']
        
    def execute(self, context):
        """
        Ejecuto el query en Lisy
        """
        if not self.hook:
            #self.hook = LisyHook(location=self.location)
            self.hook = LisyHook()
        query = 'queries/{}.json'.format(self.metodo)
        trae = self.hook.get_request(query, 'POST')
        vista = trae.json()
        pprint.pprint(vista)

class LisyQueryToJsonOperator(BaseOperator):
    """
    This operator queries on LISY API-Rest and stores to postgres db

        Args: 
            <Args de BaseOperator>
            params [diccionario]: Set de Parametros opcionales. Puede ser diccionario vacio.
            metodo [text]: El query a ejecutar, existente en Lisy.

        kwarg** Args:
            objeto [text]: Un objeto a buscar en Lisy.
        Returns:
            none
    """

    @apply_defaults
    def __init__(
        self, 
        params,
        metodo,
        *args, 
        **kwargs):
        super(LisyQueryToJsonOperator, self).__init__(*args, **kwargs)
        self.hook = None
        self.metodo = metodo
        self.params = params
        self.objeto = kwargs['objeto']
        
    def execute(self, context):
        """
        Ejecuto el query en Lisy
        """
        if not self.hook:
            self.hook = LisyHook()
        query = 'queries/{}.json'.format(self.metodo)
        trae = self.hook.get_request(query, 'POST')
        if trae.status_code == 200:
            logging.info ('::: Consulta al inventario ejecutada con exito')
            vista = trae.json()
            file_name = 'inv_query.json'
            tot_name = os.path.join('basedatos',file_name)
            logging.info('::: Base almacenada en {}'.format(tot_name))
            with open(tot_name, 'w') as outputfile:
                json.dump(vista, outputfile)
            #pprint.pprint(vista)
        else:
            logging.info ('----->>>:::FALLO EN LA CONSULTA AL INVENTARIO!!!')


class LisyCheckOperator(BaseOperator):
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
        **kwargs):
        super(LisyCheckOperator, self).__init__(*args, **kwargs)
        self.hook = None
        
    def execute(self, context):
        """
        Ejecuto el pedido del token
        """
        if not self.hook:
            #self.hook = LisyHook(location=self.location)
            self.hook = LisyHook()
        trae = self.hook.chk_token()
        vista = trae.json()
        pprint.pprint(vista)

class LisyHelpOperator(BaseOperator):
    """
    This operator display Lisy api
        Args: 
            <Args de BaseOperator>
            params [diccionario]: Set de Parametros opcionales. Puede ser diccionario vacio.

            metodo [text]: Ruta de la ayuda que se quiere visualizar. help | help/Queries and reports | help/test | help/Communication equipment | Access handling | Location unit | NI_RestInterface | Object retrieval | Service Area | Top retrieval

        kwarg** Args:
            objeto [text]: Un objeto a buscar en Lisy
            solotoken [text]: 'si' si solo quiero pedir token (para debug)
        Returns:
            none
    
    """
    @apply_defaults
    def __init__(
        self, 
        params,
        metodo='',
        *args, 
        **kwargs):
        super(LisyHelpOperator, self).__init__(*args, **kwargs)
        self.hook = None
        self.metodo = metodo
        self.params = params
        self.objeto = kwargs['objeto']
        
    def execute(self, context):
        """
        Ejecuto el query en Lisy
        """
        if not self.hook:
            #self.hook = LisyHook(location=self.location)
            self.hook = LisyHook()
        
        query = self.metodo
        trae = self.hook.get_request(query, 'GET')
        vista = trae.content
        #print (vista)

        f= open('/usr/local/airflow/api_REST.html','wb+')
        f.write(vista)
        f.close()

class LisyAboutOperator(BaseOperator):
    """
    This operator display Lisy api version
    """
    @apply_defaults
    def __init__(
        self, 
        params,
        metodo,
        *args, 
        **kwargs):
        super(LisyAboutOperator, self).__init__(*args, **kwargs)
        self.hook = None
        self.metodo = metodo
        self.params = params
        self.objeto = kwargs['objeto']
        
    def execute(self, context):
        """
        Ejecuto el query en Lisy
        """
        if not self.hook:
            #self.hook = LisyHook(location=self.location)
            self.hook = LisyHook()
        trae = self.hook.get_request('about', 'GET')
        vista = trae.json()
        pprint.pprint(vista)
