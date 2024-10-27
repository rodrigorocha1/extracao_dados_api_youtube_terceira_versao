import os
from airflow.models import Variable

# CHAVE_YOUTUBE = os.environ['CHAVE_YOUTUBE']
CHAVE_YOUTUBE = Variable.get('CHAVE_YOUTUBE')
