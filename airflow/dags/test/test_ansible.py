import os
import json
import ansible_runner


r = ansible_runner.run(private_data_dir='/usr/local/ansible/mejoras_cu1/yaml', playbook='main.yaml')
print("{}: {}".format(r.status, r.rc))

for each_host_event in r.events:
    print(each_host_event['event'])
print("Final status:")
print("La salida de ansible es: ",r.stats)
print("====================================")
print("====================================")
print("====================================")
print("====================================")

ansibleprint_raw = r.stats["failures"]
ansibleprint = json.dumps(ansibleprint_raw,indent=5,default=str)
print (ansibleprint)
print ('\n')
print ('\n')


for salidafinal in ansibleprint_raw:
    print (salidafinal)


