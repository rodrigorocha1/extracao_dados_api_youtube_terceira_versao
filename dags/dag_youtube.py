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

from operators.youtube_video_operator import YoutubeVideoOperator
from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke
from airflow.models import Variable
from pyhive import hive


def executar_comando_hive(metrica: str, path_extracao: str, nome_arquivo: str):
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
        INTO TABLE estatisticas_canais

    """
    print(query)
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
data_hora_busca = data_hora_atual.subtract(hours=3)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
data = f'extracao_data_{data}{obter_turno(hora_atual)}'

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
    caminho_path_data = data

    inicio = EmptyOperator(
        task_id="task_inicio_Dag"

    )

    busca_assunto = YoutubeBuscaOperator(
        task_id='busca_assunto',
        assunto=assunto,
        arquivo_json=ArquivoJson(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto.replace(" ","_").lower()}',
                caminho_path_data=caminho_path_data,
                metrica='requisicao_busca',
                nome_arquivo='req_busca.json',
                pasta_datalake='datalake_youtube'
        ),
        arquivo_pkl_canal=ArquivoPicke(
            camada_datalake='bronze',
            caminho_path_data=caminho_path_data,
            assunto=f'assunto_{assunto.replace(" ","_").lower()}',
            nome_arquivo='id_canais.pkl',
            pasta_datalake='datalake_youtube'
        ),
        arquivo_pkl_canal_video=ArquivoPicke(
            camada_datalake='bronze',
            assunto=f'assunto_{assunto.replace(" ","_").lower()}',
            caminho_path_data=caminho_path_data,
            nome_arquivo='id_canais_videos.pkl',
            pasta_datalake='datalake_youtube'
        ),
        operacao_hook=YoutubeBuscaAssuntoHook(assunto_pesquisa=assunto,
                                              data_publicacao=data_hora_busca)
    )

    busca_canais = YoutubeBuscaCanaisOperator(
        task_id='extracao_canal',
        assunto=assunto,
        arquivo_json=ArquivoJson(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto.replace(" ","_").lower()}',
                metrica='estatisticas_canais',
                caminho_path_data=caminho_path_data,
                nome_arquivo='req_canais.json',
                pasta_datalake='datalake_youtube'
        ),
        arquivo_pkl_canal=ArquivoPicke(
            camada_datalake='bronze',
            assunto=f'assunto_{assunto.replace(" ","_").lower()}',
            caminho_path_data=caminho_path_data,
            nome_arquivo='id_canais_brasileiros.pkl',
            pasta_datalake='datalake_youtube'
        ),
        operacao_hook=YoutubeBuscaCanaisHook(
            operacao_arquivo_pkl=ArquivoPicke(
                camada_datalake='bronze',
                caminho_path_data=caminho_path_data,
                assunto=f'assunto_{assunto.replace(" ","_").lower()}',
                nome_arquivo='id_canais.pkl',
                pasta_datalake='datalake_youtube'
            )
        )
    )

    busca_video = YoutubeVideoOperator(
        task_id='busca_videos',
        arquivo_pkl_canal_video=ArquivoPicke(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto.replace(" ","_").lower()}',
                caminho_path_data=caminho_path_data,
                nome_arquivo='id_canais_videos.pkl',
                pasta_datalake='datalake_youtube'
        ),
        assunto=assunto,
        arquivo_json=ArquivoJson(
            camada_datalake='bronze',
            assunto=f'assunto_{assunto.replace(" ","_").lower()}',
            caminho_path_data=caminho_path_data,
            metrica='estatisticas_video',
            nome_arquivo='estatisticas_video.json',
            pasta_datalake='datalake_youtube'
        ),
        operacao_hook=YoutubeVideoHook(
            carregar_canais_brasileiros=ArquivoPicke(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto.replace(" ","_").lower()}',
                caminho_path_data=caminho_path_data,
                nome_arquivo='id_canais_brasileiros.pkl',
                pasta_datalake='datalake_youtube'
            ),
            carregar_dados=ArquivoPicke(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto.replace(" ","_").lower()}',
                caminho_path_data=caminho_path_data,
                nome_arquivo='id_canais_videos.pkl',
                pasta_datalake='datalake_youtube'
            )
        ),

    )

    transformacao_canal = SparkSubmitOperator(
        task_id='spark_transformacao_dados_canais',
        conn_id='spark_default',
        application="/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/spark_etl/transform_etl.py",
        application_args=[
            '--opcao', 'C',
            '--path_extracao', caminho_path_data,
            '--path_metrica', 'estatisticas_canais',
            '--path_arquivo', 'req_canais.json'
        ],
    )
    transformacao_video = SparkSubmitOperator(
        task_id='spark_transformacao_dados_videos',
        conn_id='spark_default',
        application="/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/spark_etl/transform_etl.py",
        application_args=[
            '--opcao', 'V',
            '--path_extracao', caminho_path_data,
            '--path_metrica', 'estatisticas_video',
            '--path_arquivo', 'estatisticas_video.json'
        ],
    )

    bash_copy_docker = BashOperator(
        task_id="id_copiar_camada_prata_docker",
        bash_command='docker cp /home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/datalake_youtube/prata/ 957ec016ddf3:/opt/hive',
    )

    task_exportar_dados_canais = PythonOperator(
        task_id='salvar_dados_hive',
        python_callable=executar_comando_hive,
        op_kwargs={
            'metrica': 'estatisticas_canais',
            'path_extracao': caminho_path_data,
            'nome_arquivo': 'estatisticas_canais.parquet'
        },
        provide_context=True,
    )
    fim = EmptyOperator(
        task_id="task_fim_Dag"

    )

    inicio >> busca_assunto >> busca_canais >> busca_video >> transformacao_canal >> transformacao_video >> bash_copy_docker
    bash_copy_docker >> task_exportar_dados_canais >> fim

    # inicio >> busca_assunto >> busca_canais >> busca_video >> transformacao_canal >> fim
