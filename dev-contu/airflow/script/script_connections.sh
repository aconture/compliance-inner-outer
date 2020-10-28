airflow connections -a --conn_id postgres_conn --conn_type postgres --conn_host postgres-airflow --conn_schema airflow --conn_login airflow --conn_port 5432 --conn_password airflow

airflow connections -a --conn_id ansible_proxy --conn_type ssh --conn_host 10.9.44.173 --conn_login u565589 --conn_port 22
#declarar a mano el password para esta conexion