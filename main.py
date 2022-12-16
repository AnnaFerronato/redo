#abre o arquivo
file = open("entradaLog", "r")

#garante que está no começo do arquivo
file.seek(0)
#le as linhas e guarda em array
array = file.readlines()

#le as linhas do fim ao começo
for i in reversed(array):
    print(i)