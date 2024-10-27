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
from operators.youtube_operator import YoutubeOperator

from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from hook.youtube_hook import YotubeHook

from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca = data_hora_atual.subtract(hours=7)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')


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
    inicio = EmptyOperator(
        task_id="task_inicio_Dag"

    )

    busca_assunto = YoutubeBuscaOperator(
        task_id='id_busca_youtube',
        dados_arquivo_json=ArquivoJson(
            camada_datalake='bronze',
            assunto=assunto,
            metrica='requisicao_busca',
            nome_arquivo='req_busca.json',
            pasta_datalake='datalake_youtube'
        ),
        dados_pkl_canal=ArquivoPicke(
            camada_datalake='bronze',
            assunto=assunto,
            nome_arquivo='id_canais.pkl',
            pasta_datalake='datalake_youtube'
        ),
        operacao_hook=YoutubeBuscaAssuntoHook(
            assunto_pesquisa=assunto,
            data_publicacao=data_hora_busca,
            conn_id=None
        ),
        dados_pkl_canal_video=ArquivoPicke(
            camada_datalake='bronze',
            assunto=assunto,
            nome_arquivo='id_canais_videos.pkl',
            pasta_datalake='datalake_youtube'
        )
    )
    fim = EmptyOperator(
        task_id="task_fim_Dag"

    )

    inicio >> busca_assunto >> fim
