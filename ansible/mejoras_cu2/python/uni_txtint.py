import os
import time

dire = os.path.dirname(os.getcwd()) + '/interfaces/'
dir_compilado = os.path.dirname(os.getcwd()) + '/compilado/'
lista = os.listdir(dire)
lista.sort()

print 'Fecha actual:',time.strftime("%c")
#print 'Directorio a compilar:',dire
print 'Total de archivos a compilar:',len(lista)

if os.path.isfile(dir_compilado + 'int_total.txt'):
    nuevo_nombre = time.strftime("%y%m%d%H%M%S") + '_int_total.txt'
    os.rename(dir_compilado + 'int_total.txt', dir_compilado + nuevo_nombre)
    print 'Se renombro el archivo int_total.txt existente en el directorio',dir_compilado,'por el siguiente:', nuevo_nombre
else:
    print 'int_total.txt no existe y sera creado'

fc = open("../compilado/int_total.txt","w")
fc.write("NE|Interface|Status|Protocol|Description\n")

for x in lista:
    dira = dire + x
    fa = open(dira,"r")
    interfaces = fa.readlines()
    fa.close
    for line in interfaces:
        if line != "\n":
          if line != "shelfname|interface|portoperationalstate|protocol|info1\n":
            fc.write(line)
    print "Compilando", x

fc.close()
