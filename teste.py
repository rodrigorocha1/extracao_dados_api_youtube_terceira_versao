import sys
from datetime import datetime
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
import json
import pendulum
load_dotenv()

data_publicacao = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_publicacao = pendulum.parse(data_publicacao)
data_publicacao = data_publicacao.subtract(hours=12)
data_publicacao = data_publicacao.strftime('%Y-%m-%dT%H:%M:%SZ')

API_KEY = os.getenv('CHAVE_YOUTUBE')
youtube = build(
    'youtube',
    'v3',
    developerKey=API_KEY
)

token = True
while token:
    token = False
    print(token)


def buscar_assunto(assunto: str, pageToken=None):

    response = youtube.search().list(
        q=assunto,
        type='video',
        part='snippet',
        maxResults=50,
        publishedAfter=data_publicacao,
        pageToken=pageToken
    ).execute()
    print(response.uri)
    return response


response = buscar_assunto(
    assunto='Transport Fever 2', pageToken='')
# def executar_paginacao(response):
#     if 'nextPageToken' in response:
#         pageToken = response['nextPageToken']
#         return True, pageToken
#     return False, None


# def rodar_dag():
#     flag_paginacao = True
#     page_token = ''
#     while flag_paginacao:

#         response = buscar_assunto(
#             assunto='Transport Fever 2', pageToken=page_token)
#         print(type(response))
#         yield response

#         flag_paginacao, page_token = executar_paginacao(response=response)
#         print(flag_paginacao, page_token)


# # [0]['snippet']['description']
# for dado in rodar_dag():

#     video_ids = [item['id']['videoId'] for item in dado['items']]
#     canal_ids = [item['snippet']['channelId'] for item in dado['items']]
#     lista_canal_video = [(item['snippet']['channelId'],
#                           item['id']['videoId']) for item in dado['items']]

#     print(lista_canal_video)

# Exibir resposta formatada
# print(type(buscar_assunto(assunto='Transport Fever 2')))
# channel_id = 'UC1kCYkk99sE8mhguy_SBjmQ'

# response = youtube.channels().list(
#     part='snippet',
#     id=channel_id
# ).execute()


# response_formatado = json.dumps(response, indent=4, ensure_ascii=False)


# print(response_formatado)
# video_id = 'CljWLXS2Kc8'

# response = youtube.videos().list(
#     part='statistics,contentDetails,id,snippet,status',
#     id=video_id
# ).execute()


# response_formatado = json.dumps(response, indent=4, ensure_ascii=False)


# print(response_formatado)
