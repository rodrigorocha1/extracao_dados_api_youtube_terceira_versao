import json
from src.dados.arquivo import Arquivo
from typing import Dict
import os


class ArquivoJson(Arquivo[Dict]):

    def __init__(self, pasta_datalake, camada_datalake, caminho_path_data, assunto, nome_arquivo, metrica=None):
        #     """_summary_

        #     Args:
        #         pasta_datalake (str): o nome raíz do datalake
        #         camada_datalake (str): a camada do datalake, bronze, prata, ouro
        #         assunto (str): é o assunto de pesquisa
        #         nome_arquivo (str): métrica de pesquisa
        #         caminho_path_data (str): È o que vai armazenar a data de extracao Ex: extracao_dia_2024_10_27_15_00_00_tarde
        #         metrica (str, optional): o nome do arquivo. Defaults to None.
        #     """
        #     super().__init__(pasta_datalake, camada_datalake, assunto, nome_arquivo, metrica)
        super().__init__(pasta_datalake, camada_datalake,
                         caminho_path_data, assunto, nome_arquivo, metrica)

    def salvar_dados(self, dados: Dict):
        """Método para guardar json
        """

        if not os.path.exists(self._diretorio_completo):
            os.makedirs(self._diretorio_completo, exist_ok=True)

        with open(os.path.join(self._diretorio_completo, self._nome_arquivo), 'a') as arquivo_json:
            json.dump(dados,  arquivo_json, ensure_ascii=False)
            arquivo_json.write('\n')

    def carregar_dados(self):
        pass
