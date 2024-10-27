
from typing import Generic, Optional, TypeVar
from src.dados.ioperacoes_dados import IoperacaoDados
from abc import abstractmethod
import os

T = TypeVar('T')


class Arquivo(IoperacaoDados[T], Generic[T]):
    def __init__(self, pasta_datalake: str, camada_datalake: str, assunto: str, nome_arquivo: str, metrica: Optional[str] = None):
        """_summary_

        Args:
            pasta_datalake (str): o nome raíz do datalake
            camada_datalake (str): a camada do datalake, bronze, prata, ouro
            assunto (str): é o assunto de pesquisa
            nome_arquivo (str): o nome do arquivo
            metrica (Optional[str], optional): o nome da métrica. Defaults to None.
        """

        self._caminho_base = os.getcwd()
        self._pasta_datalake = pasta_datalake
        self._camada_datalake = camada_datalake
        self._assunto = assunto
        self._metrica = metrica
        self._nome_arquivo = nome_arquivo

        if self._metrica is not None:
            self._diretorio_completo = os.path.join(
                self._caminho_base,
                self._pasta_datalake,
                self._camada_datalake,
                self._assunto,
                self._metrica

            )
        else:
            self._diretorio_completo = os.path.join(
                self._caminho_base,
                self._pasta_datalake,
                self._camada_datalake,
                self._assunto
            )

    @abstractmethod
    def salvar_dados(self, dados: T):
        pass

    @abstractmethod
    def carregar_dados(self) -> T:
        pass
