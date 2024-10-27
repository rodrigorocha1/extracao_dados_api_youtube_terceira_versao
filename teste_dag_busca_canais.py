import pendulum

from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from hook.youtube_canais_hook import YoutubeBuscaCanaisHook
from operators.youtube_busca_operator import YoutubeBuscaOperator
from operators.youtube_canais_operator import YoutubeBuscaCanaisOperator
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

        busca_assunto = YoutubeBuscaCanaisOperator(
            task_id='extracao_canal',
            operacao_hook=YoutubeBuscaCanaisHook(
                carregar_dados=ArquivoPicke(
                    camada_datalake='bronze',
                    assunto=f'assunto_{assunto}',
                    nome_arquivo='id_canais.pkl',
                    pasta_datalake='datalake_youtube'
                ),
            ),
            dados_arquivo_json_salvar=ArquivoJson(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                metrica='estatisticas_canais',
                nome_arquivo='req_canais.json',
                pasta_datalake='datalake_youtube'
            ),
            assunto=assunto,

            dados_pkl_canal=ArquivoPicke(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                nome_arquivo='id_canais_brasileiros.pkl',
                pasta_datalake='datalake_youtube'
            )
        )

    ti = TaskInstance(task=busca_assunto)
    busca_assunto.execute(ti.task_id)