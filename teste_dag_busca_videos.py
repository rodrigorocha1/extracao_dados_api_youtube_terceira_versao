import pendulum

from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from operators.youtube_busca_operator import YoutubeBuscaOperator
from operators.youtube_video_operator import YoutubeBuscaVideoOperator
from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke
from hook.youtube_dados_videos_hook import YoutubeBuscaVideoHook


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

    caminho_path_data = f'extracao_data_{data_hora_formatada_api.replace("-", "_").replace(":", "_").replace(" ", "_")}{obter_turno(data_hora_atual.hour)}'

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
        assunto = 'python'

        busca_assunto = YoutubeBuscaVideoOperator(
            task_id='busca_video',
            assunto=assunto,
            dados_arquivo_json_salvar=ArquivoJson(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                metrica='estatisticas_canais',
                caminho_path_data=caminho_path_data,
                nome_arquivo='req_canais.json',
                pasta_datalake='datalake_youtube'
            ),
            dados_pkl_canal=ArquivoPicke(
                camada_datalake='bronze',
                caminho_path_data=caminho_path_data,
                assunto=f'assunto_{assunto}',
                nome_arquivo='id_canais.pkl',
                pasta_datalake='datalake_youtube'
            ),
            dados_pkl_canal_video=ArquivoPicke(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                caminho_path_data=caminho_path_data,
                nome_arquivo='id_canais_videos.pkl',
                pasta_datalake='datalake_youtube'
            ),
            operacao_hook=YoutubeBuscaVideoHook(

            )
        )

    ti = TaskInstance(task=busca_assunto)
    busca_assunto.execute(ti.task_id)
