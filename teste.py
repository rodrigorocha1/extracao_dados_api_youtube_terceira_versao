from datetime import datetime
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
import json
import pendulum
load_dotenv()

data_publicacao = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_publicacao = pendulum.parse(data_publicacao)
data_publicacao = data_publicacao.subtract(hours=5)
data_publicacao = data_publicacao.strftime('%Y-%m-%dT%H:%M:%SZ')

API_KEY = os.getenv('CHAVE_YOUTUBE')
youtube = build(
    'youtube',
    'v3',
    developerKey=API_KEY
)

response = youtube.search().list(
    q='transport fever',
    type='video',
    part='snippet',
    maxResults=50,
    publishedAfter=data_publicacao,
    # pageToken='CGQQAA'
).execute()


response_formatado = json.dumps(response, indent=4, ensure_ascii=False)

# Exibir resposta formatada
print(response_formatado)
channel_id = 'UC1kCYkk99sE8mhguy_SBjmQ'

response = youtube.channels().list(
    part='snippet',
    id=channel_id
).execute()


response_formatado = json.dumps(response, indent=4, ensure_ascii=False)


print(response_formatado)
video_id = 'CljWLXS2Kc8'

response = youtube.videos().list(
    part='statistics,contentDetails,id,snippet,status',
    id=video_id
).execute()


response_formatado = json.dumps(response, indent=4, ensure_ascii=False)


print(response_formatado)
