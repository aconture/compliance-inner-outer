import openpyxl
import pandas as pd
import os
import lib.teco_db
from airflow.hooks import PostgresHook

#####################################################################
#####################################################################

def gen_excel(**context):
    
    """
    Esta funcion 
        -genera un archivo excel con el contenido de los 'n' archivos csv que lee en el directorio. Requiere que todos los archivos csv tengan los mismos campos.
        Para que no repita el nombre de la solapa del excel, se debe invocar a la funcion init_report, que borra el excel que existe previamente.

        -graba el resumen del resultado en una tabla historica en la base de datos.
    
    Args: 
      none
    Returns:
      none
    
    """

    dir=context['dir']
    #archivos=os.listdir(os.path.join(os.getcwd(),dir,'auxiliar'))
    archivos = [f for f in os.listdir(os.path.join(os.getcwd(),dir,'auxiliar')) if f.endswith('.csv')]
    
    dataframe = pd.DataFrame()
    for nom_archivo in archivos:
        abspath = os.path.join(os.getcwd(),dir,'auxiliar',nom_archivo)
        dataframe_aux = pd.read_csv(abspath,delimiter=',')
        dataframe = pd.concat ([dataframe,dataframe_aux],sort=False) #sort=False para evitar un warning de Pandas
    
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

    data_resumen_dataframe.to_html('reports/auxiliar/resumen.html', index=False)

    #Guardo los registos del pivot en una tabla que contiene el historico:
    tabla = 'core_history'
    #elimino para evitar registros duplicados de la misma fecha:
    sql_delete = 'delete from core_history where fecha=\'{0}\''.format(f_ejecucion)
    lib.teco_db._delete_cursor(sql_delete)
    #registro el resumen en la tabla historico de la base de datos:
    lista_columnas = ['NE', 'finv', 'ok', 'revisar', 'fecha']
    lib.teco_db._insert_cursor (data_resumen_dataframe, tabla, lista_columnas)

    #print (dataframe)
    archivo_rep = os.path.join(os.getcwd(),dir,'reporte.xlsx')        
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

#####################################################################
#####################################################################

def _cuerpo_mail():
    
    """

    Lee el resumen generado y almacenado en reports/auxiliar/ para usarlo en el cuerpo del mail de resultado de la operacion.
    
    """

    with open('reports/auxiliar/resumen.html', 'r') as f:
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