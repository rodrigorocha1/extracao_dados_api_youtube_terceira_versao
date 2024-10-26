from typing import Optional
from abc import ABC
from airflow.providers.http.hooks.http import HttpHook
from src.dados.ioperacoes_dados import IoperacaoDados
from config.variaveis import CHAVE_YOUTUBE


class YotubeHook(HttpHook, ABC):

    def __init__(self, conn_id: str = None, carregar_dados: Optional[IoperacaoDados] = None) -> None:
        """Método para inicializar o youtube hook

        Args:
            conn_id (str, optional): id do airflow. Defaults to None.
            carregar_dados (IInfraDados, optional): tipo de carregamento de dados. Defaults to None.
        """
        self._conn_id = conn_id
        self._CHAVE = CHAVE_YOUTUBE
        self._carregar_dados = carregar_dados
        super().__init__(http_conn_id=self._conn_id)
