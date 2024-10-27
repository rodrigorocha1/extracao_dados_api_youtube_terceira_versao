try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict, Tuple
from hook.youtube_hook import YoutubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.ioperacoes_dados import IoperacaoDados


class YoutubeVideoOperator(YoutubeOperator):
    def __init__(
            self,
            dados_arquivo_json_salvar,
            dados_pkl_canal,
            operacao_hook,
            task_id,
            assunto,
            dados_pkl_canal_video=None,
            **kwargs
    ):
        super().__init__(
            dados_arquivo_json_salvar=dados_arquivo_json_salvar,
            dados_pkl_canal=dados_pkl_canal,
            operacao_hook=operacao_hook,
            task_id=task_id,
            assunto=assunto,
            dados_pkl_canal_video=dados_pkl_canal_video,
            **kwargs
        )

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            self.extracao_unica.salvar_dados(req=req)

    def execute(self, context):
        try:
            for json_response in self.ordem_extracao.run():
                self.gravar_dados(json_response)
        except:
            exit
