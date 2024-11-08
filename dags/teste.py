try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum
from hook.youtube_dados_videos_hook import YoutubeVideoHook
from operators.youtube_busca_operator import YoutubeBuscaOperator
from operators.youtube_canais_operator import YoutubeBuscaCanaisOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from hook.youtube_canais_hook import YoutubeBuscaCanaisHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from operators.youtube_video_operator import YoutubeVideoOperator
from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke
from airflow.models import Variable
from pyhive import hive
from unidecode import unidecode


def executar_comando_hive(metrica: str, path_extracao: str, nome_arquivo: str, nome_tabela: str):
    """_summary_

    Args:
        metrica (str): estatisticas_canais
        path_extracao (str): extracao_data_2024_11_02_11_49_manha
        nome_arquivo (str): estatisticas_canais.parquet
    """

    host = Variable.get('HOST_HIVE')
    port = Variable.get('PORT_HIVE')
    database = Variable.get('DATABASE_HIVE')

    conn = hive.Connection(
        host=host,
        port=port,
        database=database
    )

    cursor = conn.cursor()
    query = f"""
        LOAD DATA  INPATH '/opt/hive/prata/{metrica}/{path_extracao}/{nome_arquivo}/'
        INTO TABLE {nome_tabela}

    """

    cursor.execute(query)

    conn.close()


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
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(hours=7)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
data = f'extracao_data_{data}{obter_turno(hora_atual)}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': data_hora_busca,
}

lista_assunto = ['Python', 'Power BI',
                 'Linux', 'monster hunter',  'cities skylines']

with DAG(
    dag_id='extracao_api_youtube_debug',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    assunto = 'python'
    caminho_path_data = data

    inicio = EmptyOperator(
        task_id="task_inicio_Dag"

    )

    task_exportar_dados_videos = PythonOperator(
        task_id='salvar_dados_hive_videos',
        python_callable=executar_comando_hive,
        op_kwargs={
            'metrica': 'estatisticas_videos',
            'path_extracao': 'extracao_data_2024_11_07_noite',
            'nome_tabela': 'estatisticas_videos',
            'nome_arquivo': 'estatisticas_videos.parquet'
        },
        provide_context=True,
    )

    fim = EmptyOperator(
        task_id="task_fim_Dag"

    )

    inicio >> task_exportar_dados_videos >> fim

    # inicio >> busca_assunto >> busca_canais >> busca_video >> transformacao_canal >> fim
