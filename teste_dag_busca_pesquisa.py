

from operators.youtube_busca_operator import YoutubeBuscaOperator
from src.dados.arquivo_json import ArquivoJson
from src.dados.arquivo_pickle import ArquivoPicke


if __name__ == "__main__":
    def obter_turno(hora: int):
        if 0 <= hora < 6:
            return '_madrugada'
        elif 6 <= hora < 12:
            return '_manha'
        elif 12 <= hora < 18:
            return '_tarde'
        else:
            return '_noite'

    from airflow.models import BaseOperator, DAG, TaskInstance
    import pendulum
    from hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook

    data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
    data_hora_atual = pendulum.parse(data_hora_atual)
    data_hora_busca = data_hora_atual.subtract(hours=7)
    data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')

    data_hora_formatada_api = data_hora_atual.strftime('%Y-%m-%d %H:%M')
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

        busca_assunto = YoutubeBuscaOperator(
            task_id='id_busca_assunto',
            assunto=assunto,
            arquivo_json=ArquivoJson(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                caminho_path_data=caminho_path_data,
                metrica='requisicao_busca',
                nome_arquivo='req_busca.json',
                pasta_datalake='datalake_youtube'
            ),
            arquivo_pkl_canal=ArquivoPicke(
                camada_datalake='bronze',
                caminho_path_data=caminho_path_data,
                assunto=f'assunto_{assunto}',
                nome_arquivo='id_canais.pkl',
                pasta_datalake='datalake_youtube'
            ),
            arquivo_pkl_canal_video=ArquivoPicke(
                camada_datalake='bronze',
                assunto=f'assunto_{assunto}',
                caminho_path_data=caminho_path_data,
                nome_arquivo='id_canais_videos.pkl',
                pasta_datalake='datalake_youtube'
            ),
            operacao_hook=YoutubeBuscaAssuntoHook(assunto_pesquisa=assunto,
                                                  data_publicacao=data_hora_busca)
        )
    ti = TaskInstance(task=busca_assunto)
    busca_assunto.execute(ti.task_id)
