import openpyxl
import pandas as pd
import os
from lib.teco_db import *
from airflow.hooks import PostgresHook
from influxdb import InfluxDBClient
import json
import pprint
from datetime import datetime

#####################################################################
#####################################################################

def gen_excel(**context):
    
    """

    Esta funcion 
        -genera un archivo excel con el contenido de los 'n' archivos csv que lee en el directorio. Requiere que todos los archivos csv tengan los mismos campos.
        Para que no repita el nombre de la solapa del excel, se debe invocar a la funcion init_report, que borra el excel que existe previamente.

        -graba el resumen del resultado en una tabla historica en la base de datos.
    
    Args: 
      "dir":"path absoluto del directorio destino"
      "file":"nombre del archivo del reporte"
      "dir_html":"path absoluto del directorio destino del archivo html con el que luego se arma el email"
      "file_html":"nombre del archivo html para armar email"
       
    Returns:
      none
    """

    dir=context['dir']
    file=context['file']
    dir_html=context['dir_html']
    file_html=context['file_html']
    #archivos=os.listdir(os.path.join(os.getcwd(),dir,'auxiliar'))
    archivos = [f for f in os.listdir(os.path.join(os.getcwd(),dir,'auxiliar')) if f.endswith('.csv')]
    
    dataframe = pd.DataFrame()
    for nom_archivo in archivos:
        abspath = os.path.join(os.getcwd(),dir,'auxiliar',nom_archivo)
        dataframe_aux = pd.read_csv(abspath,delimiter=',')
        dataframe = pd.concat ([dataframe,dataframe_aux],sort=False) #sort=False para evitar un warning de Pandas
        #dataframe.to_csv('reports/auxiliar/dump{0}.dump'.format(nom_archivo), index=True)
        #print (dataframe)
        
 
    # Generacion de la solapa Resumen
    data_resumen = dataframe.pivot_table(
		index=['NE'],
		columns='EvEstado',
		aggfunc={'EvEstado':'count'},
		fill_value=0
	)    
    data_resumen.sort_values(
        by=['NE'],
        inplace=True,
        ascending=False
    )

    f_ejecucion=context['ds']

    #Convierto el pivot en dataframe y lo guardo en html para usarlo en el mail a enviar
    data_resumen_dataframe = data_resumen.reset_index()
    data_resumen_dataframe['fecha'] = f_ejecucion

    data_resumen_dataframe.to_html(dir_html+file_html, index=False)

    archivo_rep = os.path.join(os.getcwd(),dir,file) 
      
    try:
        with pd.ExcelWriter(archivo_rep,mode='a',engine='openpyxl', encoding="utf-8-sig") as escritor:
            data_resumen.to_excel(escritor, sheet_name='resumen')
            dataframe.to_excel(escritor, sheet_name='crudo', index=False)
    except FileNotFoundError:
        with pd.ExcelWriter(archivo_rep,mode='n',engine='openpyxl', encoding="utf-8-sig") as escritor:
            data_resumen.to_excel(escritor, sheet_name='resumen')
            dataframe.to_excel(escritor, sheet_name='crudo', index=False)
    finally:
        escritor.save

    #################################################################################
    ########## INI INFLUX

    #genero el dataframe para escribir en la base INFLUX, tomo como base el df
    df = dataframe.groupby(['NE', 'EvEstado'])['EvEstado'].count()        
    df = df.reset_index(name='cantidad')

    #print (df)

    data = []

    for idx in range (0, len(df)):
        #print(df['NE'][idx])
        tags = {
            "ne":'{}'.format(df['NE'][idx]),
            "EvEstado" : '{}'.format(df['EvEstado'][idx])
        }
        fields = {
            "cantidad":(df['cantidad'][idx])
        }

        body = {
            "measurement": "core_history",
            "tags": tags,
            "time": '{}'.format(datetime.now().strftime('%m/%d/%Y %H:%M:%S')),#el formato m-d-Y influy lo transforma en Y-m-d
            "fields": fields
        }
        data.append (body)
        
    #pprint.pprint(data)

    #Llama a función que inserta los datos en influx. Pasa como parametro el dataframe           
    insert_influxdb(data)


#####################################################################
#####################################################################

def _cuerpo_mail(dir1,file1):
    
    """

    Lee el resumen generado y almacenado en reports/auxiliar/ para usarlo en el cuerpo del mail de resultado de la operacion.
    
    Args: 
      "dir1":"path absoluto del directorio a buscar el archivo"
      "file1":"archivo donde esta el resumen generado"
    Returns:
      none
    """
    print(dir1)
    print(file1)
    #dir1=context['dir']
    #file1=context['file']
    #dir1_file1=dir1+file1
    #print(dir1_file1)
    #with open('/usr/local/airflow/reports/auxiliar/resumen.html', 'r') as f:
    with open(dir1+file1, 'r') as f:
        html_string = f.read()
    f.close
    return (html_string)

#####################################################################
#####################################################################

def init_report(**context):
    import os
    dir=context['dir']
    try:
        os.remove('reports/reporte.xlsx')
    except FileNotFoundError:
        pass

#####################################################################
#####################################################################

def _format_reporte_compliance(dataframe):
    dataframe = dataframe.rename(columns={
      'portoperationalstate_x':'EstadoRed',
      'portoperationalstate_y': 'EstadoLisy',
      'info1':'DescRed',
      'portinfo1':'DescLisy',
      'shelfname_x':'NE',
      'concat':'Recurso',
      'shelfhardware':'shelfHardware', #en la bd de postgres esta en minuscula, y en el dump tiene una mayuscula
      'shelfnetworkrole':'shelfNetworkRole', #en la bd de postgres esta en minuscula, y en el dump tiene una mayuscula
      'portbandwidth':'portBandwidth'
      })
    
    print ('En el formateo:',dataframe.columns)

    dataframe = dataframe [[
      'shelfNetworkRole', #tomado de Lisy
      'NE', #tomado del NE
      'shelfHardware', #tomado de Lisy
      'interface',
      'Recurso',
      'portBandwidth',
      'EstadoRed',
      'EstadoLisy',
      'DescRed',
      'DescLisy',
      'EvEstado'
    ]]
    return(dataframe)