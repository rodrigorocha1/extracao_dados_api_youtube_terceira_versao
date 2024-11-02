try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum
from operators.youtube_busca_operator import YoutubeBuscaOperator
from operators.youtube_canais_operator import YoutubeBuscaCanaisOperator

from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from hook.youtube_canais_hook import YoutubeBuscaCanaisHook

from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke


def obter_turno(hora: int):
    if 0 <= hora < 6:
        return '_madrugada'
    elif 6 <= hora < 12:
        return '_manha'
    elif 12 <= hora < 18:
        return '_tarde'
    else:
        return '_noite'


data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca = data_hora_atual.subtract(hours=12)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')

data_hora_formatada_api = data_hora_atual.strftime('%Y-%m-%d %H:%M')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': data_hora_busca,
}


with DAG(
    dag_id='extracao_api_youtube',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    assunto = 'python'
    caminho_path_data = f'extracao_data_{data_hora_formatada_api.replace("-", "_").replace(":", "_").replace(" ", "_")}{obter_turno(data_hora_atual.hour)}'

    inicio = EmptyOperator(
        task_id="task_inicio_Dag"

    )

    fim = EmptyOperator(
        task_id="task_fim_Dag"

    )

    inicio >> fim
