import json
from src.dados.arquivo import Arquivo
from typing import Dict


class ArquivoJson(Arquivo[Dict]):
    def __init__(self, pasta_datalake, camada_datalake, assunto, metrica, nome_arquivo):
        """_summary_

        Args:
            pasta_datalake (str): o nome raíz do datalake
            camada_datalake (str): a camada do datalake, bronze, prata, ouro
            assunto (str): é o assunto de pesquisa
            nome_arquivo (str): métrica de pesquisa
            metrica (str, optional): o nome do arquivo. Defaults to None.
        """
        super().__init__(pasta_datalake, camada_datalake, assunto, metrica, nome_arquivo)
        if self.__metrica is

    def salvar_dados(self):
        return super().salvar_dados()

    def carregar_dados(self):
        return super().carregar_dados()
