from airflow.models import BaseOperator
from abc import ABC, abstractmethod
from src.dados.ioperacoes_dados import IoperacaoDados
from hook.youtube_hook import YotubeHook
from typing import Dict


class YoutubeOperator(BaseOperator, ABC):
    def __init__(self, operacao_hook: YotubeHook, task_id: str,  ** kwargs):

        self._operacao_hook = operacao_hook
        super().__init__(task_id=task_id, **kwargs)

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
