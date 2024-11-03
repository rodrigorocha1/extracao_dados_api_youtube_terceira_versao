from pyhive import hive
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


def executar_comando_hive(metrica: str, path_extracao: str, nome_arquivo: str):
    """_summary_

    Args:
        metrica (str): estatisticas_canais
        path_extracao (str): extracao_data_2024_11_02_11_49_manha
        nome_arquivo (str): estatisticas_canais.parquet
    """

    host = os.environ['HOST_HIVE']
    port = os.environ['PORT_HIVE']
    database = os.environ['DATABASE_HIVE']

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


executar_comando_hive(metrica='estatisticas_canais',
                      path_extracao='extracao_data_2024_11_02_noite', nome_arquivo='estatisticas_canais.parquet')
