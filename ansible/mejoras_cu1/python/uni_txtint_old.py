import os

dire = os.path.dirname(os.getcwd()) + "/interfaces/"
lista = os.listdir(dire)
lista.sort()
print 'directorio de interfaces:',dire
print 'Total de archivos a compilar:',len(lista)

fc = open("../compilado/int_total.txt","w")
fc.write("NE|Interface|Status|Protocol|Description\n")

for x in lista:
    dira = dire + x
    fa = open(dira,"r")
    fc.write(fa.read())
    fa.close()
    print "Compilando archivo:", x

fc.close()
