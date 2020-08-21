from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults


#my hook
from clima_plugin.hooks.clima_hook import ClimaHook

import logging
import pprint

class DisplayClimaOperator(BaseOperator):
    """
    This operator display weather from https://openweathermap.org/ api
    """
    #template_fields = ['first_contact_date', 'bulk_file']
    #template_ext = ['.csv']

    #ver la variable template_fields propia de airflow
    template_fields = ['location', 'params']

    @apply_defaults
    def __init__(
        self, 
        params,
        location,
        api_key,
        *args, 
        **kwargs):
        super(DisplayClimaOperator, self).__init__(*args, **kwargs)
        self.hook = None
        self.location = location
        self.api_key = api_key
        self.params = params
        
        
        
        
    def execute(self, context):
        """
        Display weather
        """
        if not self.hook:
            self.hook = ClimaHook(location=self.location, api_key=self.api_key)
        r = self.hook.get_weather()
        pprint.pprint(r)
        print(self.location)
        print(self.params)
        print(self.api_key)
        
        
        
        