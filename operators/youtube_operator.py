from airflow.models import BaseOperator
from abc import ABC, abstractmethod
from src.dados.ioperacoes_dados import IoperacaoDados
from hook.youtube_hook import YotubeHook
from typing import Dict, Optional


class YoutubeOperator(BaseOperator, ABC):
    def __init__(
        self,
        dados_arquivo_json_salvar: IoperacaoDados,
        dados_pkl_canal: IoperacaoDados,
        operacao_hook: YotubeHook,
        task_id: str,
        assunto: str,

        dados_pkl_canal_video: Optional[IoperacaoDados] = None,
        **kwargs
    ):
        self._assunto = assunto

        self._dados_arquivo_json_salvar_req = dados_arquivo_json_salvar
        self._operacao_dados_pkl_canal = dados_pkl_canal
        self._operacao_dados_pkl_canal_video = dados_pkl_canal_video
        self._operacao_hook = operacao_hook
        super().__init__(task_id=task_id, **kwargs)

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
