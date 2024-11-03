try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict, Tuple

from operators.youtube_operator import YoutubeOperator
from src.dados.ioperacoes_dados import IoperacaoDados


class YoutubeVideoOperator(YoutubeOperator):
    def __init__(self, task_id, assunto, operacao_hook, arquivo_json, arquivo_pkl_canal=None, arquivo_pkl_canal_video=None, **kwargs):
        super().__init__(task_id=task_id, assunto=assunto, operacao_hook=operacao_hook, arquivo_json=arquivo_json,
                         arquivo_pkl_canal=arquivo_pkl_canal, arquivo_pkl_canal_video=arquivo_pkl_canal_video, **kwargs)

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            req['assunto'] = self._assunto
            self._arquivo_json.salvar_dados(dados=req)

    def execute(self, context):
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(json_response)
        except Exception as E:
            print(E)
            exit
