import pendulum

from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from operators.youtube_busca_operator import YoutubeBuscaOperator
from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke


if __name__ == "__main__":
    from airflow.models import BaseOperator, DAG, TaskInstance

    data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
    data_hora_atual = pendulum.parse(data_hora_atual)
    data_hora_busca = data_hora_atual.subtract(hours=7)
    data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')

    data_hora_formatada_api = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')

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

        busca_assunto = YoutubeBuscaOperator(

            assunto=assunto,

            task_id='id_busca_youtube',
            dados_arquivo_json_salvar=ArquivoJson(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                metrica='requisicao_busca',
                nome_arquivo='req_busca.json',
                pasta_datalake='datalake_youtube'
            ),
            dados_pkl_canal=ArquivoPicke(
                camada_datalake='bronze',
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
                nome_arquivo='id_canais_videos.pkl',
                pasta_datalake='datalake_youtube'
            )
        )

    ti = TaskInstance(task=busca_assunto)
    busca_assunto.execute(ti.task_id)
