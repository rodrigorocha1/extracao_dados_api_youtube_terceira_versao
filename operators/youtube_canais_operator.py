from typing import Dict, Optional, Tuple
from hook.youtube_hook import YotubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.ioperacoes_dados import IoperacaoDados


class YoutubeBuscaCanaisOperator(YoutubeOperator):

    def __init__(
            self,
            task_id: str,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: IoperacaoDados,
            arquivo_pkl_canal: IoperacaoDados,
            arquivo_pkl_canal_video: Optional[IoperacaoDados] = None,
            **kwargs
    ):
        super().__init__(
            task_id=task_id,
            assunto=assunto,
            operacao_hook=operacao_hook,
            arquivo_json=arquivo_json,
            arquivo_pkl_canal_video=arquivo_pkl_canal_video,
            arquivo_pkl_canal=arquivo_pkl_canal,
            **kwargs
        )

    def gravar_dados(self, req: Dict):
        try:
            if len(req['items']) > 0 and req['items'][0]['snippet']['country'] == 'BR':
                id_canal = req['items'][0]['id']

                req['assunto'] = self._assunto

                self._arquivo_json.salvar_dados(dados=req)
                self._arquivo_pkl_canal.salvar_dados(id_canal)
        except:
            pass

    def execute(self, context):
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(json_response)
        except Exception as E:
            print(E)
            exit
