FROM apache/airflow:1.10.12-python3.7
USER root
#sshpass para ejecutar comando remoto con la pwd en una variable de entorno
RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
	iputils-ping \
	openssh-server \
	sshpass \
    && apt-get autoremove -yqq --purge \
    && apt-get clean
RUN /usr/local/bin/python -m pip install --upgrade pip

RUN pip install ansible==2.9.11
#	pip install ansible-runner==1.4.6
	
RUN	pip install bottle==0.12.18 \
	&& pip install openpyxl==3.0.3
RUN mkdir /etc/ansible && chmod +755 /etc/ansible
RUN chmod 777 /usr/local/airflow
#RUN mkdir /root/.ssh && chmod 600 /root/.ssh
COPY ansible/ansible.cfg /etc/ansible
COPY ansible/hosts /etc/ansible
COPY airflow/airflow.cfg /opt/airflow	
USER airflow
