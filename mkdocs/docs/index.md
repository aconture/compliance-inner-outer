# Automation
![](https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcQOkRCRtWwpS2Do6YLvuMMPa8A6dvjlaOetQg&usqp=CAU)

## Introducción

COMPLIANCE INNER & OUTER: ejecuta chequeos de validación de la información que existe entre el inventario de Telecom y los elementos de red Inner Core y Outer Cre.

## Procedimiento

### El procedimiento para que levanten las aplicaciones:

Parado en el directorio donde se encuentra "docker-compose.yml", ejecutar docker-compose up -d
Verificar en el browser que levantó la app 'airflow', en la dirección http://172.29.14.123:8080
El procedimiento para ejecutar el dag de 'compliance'

### Conexiones y variables para el DAG de Compliance:

Ingresar al docker del webserver: #docker exec -it bash ==> el id del docker se obtiene con 'docker ps'
Una vez dentro de la imagen que está corriendo, ejecutar los comandos de airflow/script/script_connections.sh
Objetos de la base de datos para el DAG de Compliance:
Desde Windows, ingresar al docker de Postgres: #docker exec -it sh ==> el id del docker se obtiene con 'docker ps'
Una vez dentro de la imagen que está corriendo, ejecutar lo siguiente: '# psql -U airflow ==> conexión a la base ' airflow=# ==> sobre el prompt de la base, ejecutar los comandos que están en airflow/script/script_bd.sql

#### Conexión a infra desde Ansible

* Test

### Build and Test

Acceder en el browser a http://172.29.14.123:8080/admin/
Ejecutar el DAG "Compliance_Inner_Outer"

### Monitoring

Prometheus: http://prometheus.infra.cablevision-labs.com.ar:9090/graph

Grafana: http://grafana.infra.cablevision-labs.com.ar:3000/login

User/Pass: telecom/telecom

### Repositorio GIT

Disponiendo de un user y pass de GIT hub, se debe solicitar los respectivos permisos.
https://github.com/aconture/compliance-inner-outer.git. En este respositorio se puede tener diferentes branchs por lo cual se debera utilizar la ultima verisón disponible:




A partir de eso, desde el directorio donde se va a desplegar el ambiente, ejecutar:
git clone https://github.com/aconture/compliance-inner-outer.git


    test (){
        prueba2 (test);

}




