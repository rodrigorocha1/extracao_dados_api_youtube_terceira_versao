from typing import Dict, Tuple
from hook.youtube_hook import YotubeHook
from operators.youtube_operator import YoutubeOperator
from src.dados.ioperacoes_dados import IoperacaoDados


class YoutubeBuscaCanaisOperator(YoutubeOperator):

    def __init__(
            self,
            dados_arquivo_json_salvar: IoperacaoDados,
            dados_pkl_canal: IoperacaoDados,
            operacao_hook: YotubeHook,
            task_id,
            assunto=str,

            dados_pkl_canal_video=None,
            **kwargs
    ):
        super().__init__(
            dados_arquivo_json_salvar=dados_arquivo_json_salvar,
            dados_pkl_canal=dados_pkl_canal,
            operacao_hook=operacao_hook,
            task_id=task_id,
            dados_pkl_canal_video=dados_pkl_canal_video,
            assunto=assunto,

            **kwargs
        )

    def gravar_dados(self, req: Dict):
        try:
            if len(req['items']) > 0 and req['items'][0]['snippet']['country'] == 'BR':
                id_canal = req['items'][0]['id']

                req['assunto'] = self._assunto

                self._dados_arquivo_json_salvar_req.salvar_dados(dados=req)
                self._operacao_dados_pkl_canal.salvar_dados(id_canal)
        except:
            pass

    def execute(self, context):
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(json_response)
        except Exception as E:
            print(E)
            exit
