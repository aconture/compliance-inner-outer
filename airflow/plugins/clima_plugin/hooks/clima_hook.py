from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

#from airflow.models import Variable

# import external libraries to interact with target system
# ...

import requests
import sys
import logging

#API_KEY_DEFAULT="1ae1b784e7fed83622638bd8c9384d94"
#LOCATION_DEFAULT="Buenos Aires"

class ClimaHook(BaseHook):
    """
    Hook to interact with the OpenWeatherMap API.
    """

    def __init__(self, location, api_key, *args, **kwargs):
        #self.api_key = API_KEY_DEFAULT
        self.api_key = api_key
        #self.location = LOCATION_DEFAULT
        self.location=location
        
    
    def get_weather(self):
        url = "https://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid={}".format(self.location, self.api_key)
        r = requests.get(url)
        #print ('Estoy en el hook')
        #logging.info(r.json())
        return r.json()


