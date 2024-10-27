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

data_hora_formatada_api = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': data_hora_busca,
}

caminho_path_data = f'extracao_data_{data_hora_formatada_api.replace("-", "_").replace(":", "_").replace(" ", "_")}{obter_turno(data_hora_atual.hour)}'

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

        assunto=assunto,

        task_id='id_busca_youtube',
        dados_arquivo_json_salvar=ArquivoJson(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                caminho_path_data=caminho_path_data,
                metrica='requisicao_busca',
                nome_arquivo='req_busca.json',
                pasta_datalake='datalake_youtube'
        ),
        dados_pkl_canal=ArquivoPicke(
            camada_datalake='bronze',
            caminho_path_data=caminho_path_data,
            assunto=f'assunto_{assunto}',
            nome_arquivo='id_canais.pkl',
            pasta_datalake='datalake_youtube'
        ),
        operacao_hook=YoutubeBuscaAssuntoHook(
            assunto_pesquisa=f'{assunto}',
            data_publicacao=data_hora_busca,
            conn_id=None
        ),
        dados_pkl_canal_video=ArquivoPicke(
            camada_datalake='bronze',
            assunto=f'assunto_{assunto}',
            caminho_path_data=caminho_path_data,
            nome_arquivo='id_canais_videos.pkl',
            pasta_datalake='datalake_youtube'
        )
    )

    busca_dados_canais = YoutubeBuscaCanaisOperator(
        task_id='extracao_canal',
        operacao_hook=YoutubeBuscaCanaisHook(
            carregar_dados=ArquivoPicke(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                caminho_path_data=caminho_path_data,
                nome_arquivo='id_canais.pkl',
                pasta_datalake='datalake_youtube'
            ),
        ),
        dados_arquivo_json_salvar=ArquivoJson(
            camada_datalake='bronze',
            assunto=f'assunto_{assunto}',
            metrica='estatisticas_canais',
            caminho_path_data=caminho_path_data,
            nome_arquivo='req_canais.json',
            pasta_datalake='datalake_youtube'
        ),
        assunto=assunto,

        dados_pkl_canal=ArquivoPicke(
            camada_datalake='bronze',
            assunto=f'assunto_{assunto}',
            caminho_path_data=caminho_path_data,
            nome_arquivo='id_canais_brasileiros.pkl',
            pasta_datalake='datalake_youtube'
        )
    )

    fim = EmptyOperator(
        task_id="task_fim_Dag"

    )

    inicio >> busca_assunto >> busca_dados_canais >> fim
