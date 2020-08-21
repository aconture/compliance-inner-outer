#!/usr/bin/env python

import ansible_runner
import pprint

# private_data_dir (str) – The directory containing all runner metadata needed to invoke the runner module. Output artifacts will also be stored here for later consumption.
r = ansible_runner.run(private_data_dir='/tmp/resultado', playbook='/usr/local/airflow/ansible/test1.yml')
print('\nResultado de la ejecucion:')
print('{}: {}'.format(r.status, r.rc))
# successful: 0

print('\nEventos registrados:')
for each_host_event in r.events:
    print(each_host_event['event'])

print('\nInformacion final:')
pprint.pprint(r.stats)

