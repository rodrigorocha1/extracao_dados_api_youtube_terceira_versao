import json

from src.dados.arquivo_pickle import ArquivoPicke

# Abrir e carregar o arquivo JSON
with open('/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/datalake_youtube/bronze/assunto_python/requisicao_busca/req_busca.json/req_busca.json', 'r') as arquivo:
    dados = json.load(arquivo)

# Exibir os dados
print(dados)


apkl = ArquivoPicke(
    camada_datalake='bronze',
    assunto=f'assunto_python',
    nome_arquivo='id_canais.pkl',
    pasta_datalake='datalake_youtube'
)


lista_canal_video = [
    (
        item['snippet']['channelId'],
        item['id']['videoId']
    )
    for item in dados['items']
]


print(lista_canal_video)
