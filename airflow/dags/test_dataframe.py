import openpyxl
import pandas as pd
import os
import lib.teco_db
from airflow.hooks import PostgresHook
from influxdb import InfluxDBClient
import json
import pprint
from datetime import datetime

dir='/usr/local/airflow/reports'
#archivos=os.listdir(os.path.join(os.getcwd(),dir,'auxiliar'))
archivos = [f for f in os.listdir(os.path.join(os.getcwd(),dir,'auxiliar')) if f.endswith('.csv')]

dataframe = pd.DataFrame()
for nom_archivo in archivos:
    abspath = os.path.join(os.getcwd(),dir,'auxiliar',nom_archivo)
    dataframe_aux = pd.read_csv(abspath,delimiter=',')
    dataframe = pd.concat ([dataframe,dataframe_aux],sort=False) #sort=False para evitar un warning de Pandas
    #dataframe.to_csv('reports/auxiliar/dump{0}.dump'.format(nom_archivo), index=True)
    #print (dataframe)
    
df = dataframe.groupby(['NE', 'EvEstado'])['EvEstado'].count()
df = df.reset_index(name='cantidad')

#df.columns = [x.capitalize() for x in df.columns]

print (df)

data = []

for idx in range (0, len(df)):
    #print(df['NE'][idx])
    tags = {
        "ne":'{}'.format(df['NE'][idx]),
        "EvEstado" : '{}'.format(df['EvEstado'][idx])
    }
    fields = {
        "cantidad":'{}'.format(df['cantidad'][idx])
    }

    body = {
        "measurement": "core_history",
        "tags": tags,
        #"time":'2020-11-11T8:01:00Z',
        "time": '{}'.format(datetime.now().strftime('%d/%m/%Y %H:%M:%S')),
        "fields": fields
    }
    
    #data.append ('measurement:core_history')
    data.append (body)
    

pprint.pprint(data)


client = InfluxDBClient(host='influxdb', port=8086, username='admin', password='Welcome1')
client.create_database('test_contu')
client.switch_database('test_contu')
# client.write_points([json_str])
client.write_points(data)


#curl -G 'http://172.29.14.123:8086/query?pretty=true' --data-urlencode "db=test_contu" --data-urlencode "q=select * from core_history;"
