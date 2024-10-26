from typing import Optional, Dict
from abc import ABC, abstractmethod
from airflow.providers.http.hooks.http import HttpHook
from src.dados.ioperacoes_dados import IoperacaoDados
from config.variaveis import CHAVE_YOUTUBE
from googleapiclient.discovery import build


class YotubeHook(HttpHook, ABC):

    def __init__(self, conn_id: str, carregar_dados: Optional[IoperacaoDados] = None) -> None:
        """Método para inicializar o youtube hook

        Args:
            conn_id (str, optional): id do airflow. Defaults to None.
            carregar_dados (IInfraDados, optional): tipo de carregamento de dados. Defaults to None.
        """
        self._conn_id = conn_id
        self._CHAVE = CHAVE_YOUTUBE
        self._carregar_dados = carregar_dados
        self._youtube = build(
            'youtube',
            'v3',
            developerKey=self._CHAVE
        )
        super().__init__(http_conn_id=self._conn_id)

    def _executar_paginacao(self, response: Dict):
        if 'nextPageToken' in response:
            pageToken = response['nextPageToken']
            return True, pageToken
        return False, None

    @abstractmethod
    def conectar_api(self, **kwargs) -> Dict:

        pass

    @abstractmethod
    def rodar_dag(self):
        pass
