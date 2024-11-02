import os
from airflow.models import Variable

# CHAVE_YOUTUBE = os.environ['CHAVE_YOUTUBE']
URL = 'https://youtube.googleapis.com/youtube/v3'
CHAVE_YOUTUBE = Variable.get('CHAVE_YOUTUBE')
