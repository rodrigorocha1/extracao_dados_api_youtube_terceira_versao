import pickle

with open('id_canais.pkl', 'rb') as arquivo_pickle:
    lista = pickle.load(arquivo_pickle)


print(lista)
