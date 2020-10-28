# Introducción 
COMPLIANCE INNER & OUTER: ejecuta chequeos de validación de la información que existe entre el inventario de Telecom y los elementos de red Inner Core y Outer Cre. 

# Procedimiento
## El procedimiento para que levanten las aplicaciones:
1.	Instalar docker-desktop (windows)
2.  Parado en el directorio donde se encuentra "docker-compose.yml", ejecutar docker-compose up -d
3.  Verificar en el browser que levantó la app 'airflow', en la dirección http://localhost:8080

## El procedimiento para ejecutar el dag de 'compliance'

###	Conexiones y variables para el DAG de Compliance:
1.  Desde Windows, ingresar al docker del webserver: #docker exec -it <id del docker> bash ==> el id del docker se obtiene con 'docker ps'
2.  Una vez dentro de la imagen que está corriendo, ejecutar los comandos de airflow/script/script_connections.sh

###	Objetos de la base de datos para el DAG de Compliance:
1.  Desde Windows, ingresar al docker de Postgres: #docker exec -it <id del docker> sh ==> el id del docker se obtiene con 'docker ps'
2.  Una vez dentro de la imagen que está corriendo, ejecutar lo siguiente:
	'# psql -U airflow ==> conexión a la base
	' airflow=# ==> sobre el prompt de la base, ejecutar los comandos que están en airflow/script/script_bd.sql

# Build and Test
1.  Acceder en el browser a http://localhost:8080/admin/
2.  Ejecutar el DAG "Compliance_Inner_Outer"

# Contribute
git clone https://macastano@dev.azure.com/macastano/Automatizaci%C3%B3n%20in-house%20CTO/_git/compliance-inner-outer

