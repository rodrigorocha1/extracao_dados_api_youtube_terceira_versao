import pendulum
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from operators.youtube_busca_operator import YoutubeBuscaOperator
from operators.youtube_video_operator import YoutubeVideoOperator
from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke


from hook.youtube_dados_videos_hook import YoutubeVideoHook


if __name__ == "__main__":

    from airflow.models import BaseOperator, DAG, TaskInstance

    data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
    data_hora_atual = pendulum.parse(data_hora_atual)
    data_hora_busca = data_hora_atual.subtract(hours=7)
    data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')

    data_hora_formatada_api = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')

    def obter_turno(hora: int):
        if 0 <= hora < 6:
            return '_madrugada'
        elif 6 <= hora < 12:
            return '_manha'
        elif 12 <= hora < 18:
            return '_tarde'
        else:
            return '_noite'

    caminho_path_data = 'extracao_data_2024_11_02_11_49_manha'

    def obter_turno(hora: int):
        if 0 <= hora < 6:
            return '_madrugada'
        elif 6 <= hora < 12:
            return '_manha'
        elif 12 <= hora < 18:
            return '_tarde'
        else:
            return '_noite'

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
        assunto = 'Power BI'

        busca_assunto = YoutubeVideoOperator(
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

    ti = TaskInstance(task=busca_assunto)
    busca_assunto.execute(ti.task_id)
