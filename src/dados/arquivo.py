
from typing import Generic, TypeVar
from src.dados.ioperacoes_dados import IoperacaoDados
from abc import abstractmethod
import os

T = TypeVar('T')


class Arquivo(IoperacaoDados, Generic[T]):
    def __init__(self, pasta_datalake: str, camada_datalake: str, assunto: str, metrica: str, nome_arquivo: str):
        """init para arquivo

        Args:
            pasta_datalake (str): o nome raíz do datalake
            camada_datalake (str): a camada do datalake, bronze, prata, outro
            assunto (str): é o assunto de pesquisa
            metrica (str): métrica de pesquisa
            nome_arquivo (str): o nome do arquivo
        """
        self.__caminho_base = os.getcwd()
        self.__pasta_datalake = pasta_datalake
        self.__camada_datalake = camada_datalake
        self.__assunto = assunto
        self.__metrica = metrica
        self.__nome_arquivo = nome_arquivo

    @abstractmethod
    def salvar_dados(self) -> None:
        pass

    @abstractmethod
    def carregar_dados(self) -> T:
        pass
