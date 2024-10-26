from airflow.models import BaseOperator
from abc import ABC, abstractmethod
from src.dados.ioperacoes_dados import IoperacaoDados
from hook.youtube_hook import YotubeHook
from typing import Dict


class YoutubeOperator(BaseOperator, ABC):
    def __init__(self, **kwargs):
        self._operacao_dados = IoperacaoDados()
        self._operacao_hook = YotubeHook()
        super().__init__(**kwargs)

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
