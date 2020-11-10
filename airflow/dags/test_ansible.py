import os
import json
import ansible_runner

#os.system ('rm /usr/local/ansible/mejoras_cu1/interfaces/*.txt; cd /usr/local/ansible/mejoras_cu1/yaml; ansible-playbook main.yaml')
#os.system ('rm /usr/local/ansible/mejoras_cu1/interfaces/*.txt')
r = ansible_runner.run(private_data_dir='/usr/local/ansible/mejoras_cu1/yaml', playbook='main.yaml')
print("{}: {}".format(r.status, r.rc))
# successful: 0
for each_host_event in r.events:
    print(each_host_event['event'])
print("Final status:")
print("====================================")
print("====================================")
print("La salida de ansible es: ",r.stats)
print("====================================")
print("====================================")
ansibleprint_raw = ansibleprint_raw.replace("\'", "\"")
ansibleprint = json.loads(r.stats)
print("SALIDA REDUCIDA: ",ansibleprint)   

