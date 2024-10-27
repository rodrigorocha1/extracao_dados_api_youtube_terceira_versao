from airflow.models import BaseOperator
from abc import ABC, abstractmethod
from src.dados.ioperacoes_dados import IoperacaoDados
from hook.youtube_hook import YotubeHook
from typing import Dict, Optional


class YoutubeOperator(BaseOperator, ABC):
    def __init__(
            self,
            task_id,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: IoperacaoDados,
            arquivo_pkl_canal_video: IoperacaoDados,
            arquivo_pkl_canal: IoperacaoDados,
            **kwargs
    ):
        self._operacao_hook = operacao_hook
        self._arquivo_json = arquivo_json
        self._asunto = assunto
        self._arquivo_pkl_canal_video = arquivo_pkl_canal_video
        self._arquivo_pkl_canal = arquivo_pkl_canal
        super().__init__(task_id=task_id, **kwargs)

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
