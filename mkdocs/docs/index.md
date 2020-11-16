# Automation
![](https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcQOkRCRtWwpS2Do6YLvuMMPa8A6dvjlaOetQg&usqp=CAU)

![](https://imgur.com/EkI6427)

## Arquitectura de la solución

![](https://i.imgur.com/EkI6427.png[/img)

## Casos de uso

### Compliance de Inner y Outer Core contra Inventario LISY

#### Introdución

COMPLIANCE INNER & OUTER: ejecuta chequeos de validación de la información que existe entre el inventario de Telecom y los elementos de red Inner Core y Outer Core.

#### Pre-requisitos
* El framework de automatización fue desplegado inicialmente en un ambiente de laboratorio. Para ello, se utilizo una maquina virtual con una imagen LINUX con las siguientes caracteristicas: 

    | Parametro | Valor |
    | --- | --- |
    | CPU | xxx vCPUs |
    |Memoria| xx Gbps |
    |Disco| xxx Gbps  |

* Disponer de una cuenta en github y con ello poder subir/bajar codigo. Actualmente se esta utilizando un repositorio privado de github por lo que se debera solicitar autorización al dueño del repositorio (integrante del equipo) y el mismo pueda ser compartido.

* Adicionalmente, dado que se dispone de un equipo conformado por varias personas, para lo que es el desarrollo de las diferentes funciones, cada integrante puede desplegar la solución en su propia PC y a partir de ahi poder hacer las diferentes pruebas. Los requerimientos para ello son:

    + ** Windows: **
Disponer o instalar previamente Docker Desktop en la PC donde se desplegara la solución. Si bien utilizar windows es una alternativa, es recomendable el uso de linux, por un tema de recursos de la PC. 

    * **Linux**:
Disponer de cualquiera de las distribuciones, y disponer de conexion a internet para la descarga de las diferentes actualizaciones.



#### Procedimiento para despliegue del ambiente
A partir de la arquitectura propuesta, los diferentes componentes son desplegados mediante la herramienta multicontenedor docker-compose en donde, a traves del archivo YAML, permite definir y configurar los servicios, volúmenes y redes que utilizará en el entorno. Como se indico, el ambiente esta desplegado en una VM de laboratorio, sin embargo, el mismo tambien puede ser desplegado en otros entornos y permitir un ambiente colaboratorivo en el desarrollo. Para ello, a continuación se indica los pasos necesarios para desplegar el entorno: 

* **Windows**: 
    1. Crear un nueva carpeta donde de alojaran los diferentes archivos de la solcuión. Esta nueva carpeta debera ser creada en el directorio publico de la unidad C --> C:\Users\Public. 

    2. Mediante CMD, cambiar y ubicarse en el nuevo directorio/carpeta que fue creado en el paso previo.

    3. Una vez en ese lugar, proceder a hacer un clone del reposiorio GIT. Dependiendo del branch que se este utilizando, considerar la version correcta el momento de hacer el git clone. Por ejm, para el caso de la v1.3, el git clone sera:

        ```git clone --branch 1.3 https://github.com/aconture/compliance-inner-outer.git
        ```


    4. Este git clone descargara todos los archivos que contienen la capeta "compliance-inner-outer".    Moviendose a ese directorio, se tendra el archivo docker-compose.yaml disponible.
    5. En ese directorio donde se encuentra el archivo docker-compose.yaml se procede a ejecutar:

        ```docker-compose up -d
        ```

    6. Una vez que despliegue todos los contenedores con sus respectivos parametros, se procedera a verficar que todos ellos estan corriendo.

        ```docker ps
        ```

    7. Cada aplicacion que fue definida en el docker-compose.yaml, tiene su dirección IP y puerto. A partir de esa información, mediante un navegador web se podra acceder a cada una de ellas. Las direcciones IP con sus puertos son:

        * Airflow: http://172.29.14.123:8080
        * Grafana: http://172.29.14.123:3000
        * Mkdocs:  http://172.29.14.123:8000

### Conexiones y variables para el DAG de Compliance:

#### Conexión con la base de datos


Una vez desplegado en ambiente, es necesario realizar las conexiones contra la base de datos. Para ello se debe ingresar al docker del webserver y agregar la info necesaria para esa conexión. Desde el repositorio donde se desplego los componentes, ejecutar:
~~~
#docker exec -it "ID del docker" bash
~~~

 El id del docker se obtiene con **docker ps**
 
Una vez dentro de la imagen que está corriendo, ejecutar los comandos de airflow/script/script_connections.sh
O se puede ir hasta el repositorio donde esta alojado el contenido de script_connections.sh (compliance-inner-outer\airflow\script#) y copiar ese contenido en el docker que previamente se lo ejecuto.

#### Objetos de la base de datos para el DAG de Compliance
Para lo que es el contenido de la base de datos, igualmente, desde el repositorio donde se desplegaron loc componentes, se debe ingresar al docker de postgres y ejecutar:
~~~
 #docker exec -it "ID del docker" sh 
~~~
El id del docker se obtiene con 'docker ps'

Una vez dentro de la imagen que está corriendo, ejecutar lo siguiente: 
~~~
# psql -U airflow
~~~
Una vez en la ubicacion **airflow=#**, sobre el prompt de la base, ejecutar los comandos que están en airflow/script/script_bd.sql. Eventualmente, sobre esta misma ubicacion de **airflow=#** tambien se puede hacer un "copiar" y "pegar" del contenido que esta en el **script_bd.sql**. Esta base datos se encuentra en el repositorio "compliance-inner-outer\airflow\script>"

#### Archivo faltante, necesario para correr el caso de uso
El momento de hacer el git clone, se verificó que el archivo **EthernetPortsByIpShelf.txt** no fue descargado, por lo cual, es necesario añadirlo manualmente en el repositorio **compliance-inner-outer\airflow\Inner#**

#### Conexión a infra desde Ansible

Cada vez que se corre el caso de uso, se hace una consulta a los diferentes equipos de Inner y Outer Core, sin embargo, para evitar tener que estar consultando a los equipos en producción, dentro del codigo de conexión se realizó un objeto de prueba o mock en el codigo. Por defecto se consulta a los equipos, con lo cual es necesario ir al archivo **Inner_Compliance_modular.py** que se encuentra ubicado en **compliance-inner-outer\airflow\dags#** y se debe buscar la funcion **auto_ansible** y en la línea **'mock':True** cambiar a **'mock':False**

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


### FUNCIONES UTILIZADAS Y DESARROLLADAS EN CASO DE USO


~~~~
naming_inv : Función que modifica el contenido de ciertos campos traidos desde el inventario para que puedan ser comparados con el archivo que trae ansible desde el NE
~~~~

naming_inv : 

~~~~
Función que modifica el contenido de ciertos campos traidos desde el inventario para que puedan ser comparados con el archivo que trae ansible desde el NE
~~~~


~~~
naming_ne : Esta funcion modifica el contenido de ciertos campos traidos desde los NE, se queda con los puertos indicados en 'whitelist' y filtra los puertos indicados en 'blacklist' no deseados en el análisis.
~~~


~~~
_logic_compl_inventario : Esta funcion ejecuta las consultas a la base, con las condiciones que vienen en la estructura 'struct'
~~~

~~~
Caso_ok_v2 : Esta funcion determina los registros correctamente sincronizados entre el NE y el inventario.
~~~


~~~~
Caso2_revisar_2 : Esta funcion determina los registros que hay que revisar entre el NE y el inventario, por tener estados inconsistentes.
~~~~

~~~
Caso_ok_reserva: Esta funcion determina los registros que ???????
~~~

~~~
Caso_na: Esta funcion determina los registros que tienen un estado N/A.
~~~

~~~
Caso3_ne_inv: Esta funcion determina los registros que existen en el NE y NO existen en el inventario.
~~~

~~~
Caso_inv_ne: Esta funcion determina los registros que existen en el Inventario y NO existen en el NE.
~~~
Caso_inv_ne


A partir de eso, desde el directorio donde se va a desplegar el ambiente, ejecutar:
git clone https://github.com/aconture/compliance-inner-outer.git


    test (){
        prueba2 (test);

}


| prueba 1 | prueba 2 |
| -------- | -------- |
| prueba 1 | prueba 2 |

~~~
texto
~~~

~~~~~~~~~~~~~~~~~~~~~~~~~~~~ .html
<p>paragraph <b>emphasis</b>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

markdown_extensions:
    - CodeHilite


Apple
:   Pomaceous fruit of plants of the genus Malus in 
    the family Rosaceae.

Orange
:   The fruit of an evergreen tree of the genus Citrus.

1. Prueba
