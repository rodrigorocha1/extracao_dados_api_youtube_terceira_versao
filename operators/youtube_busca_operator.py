
from operators.youtube_operator import YoutubeOperator
from hook.youtube_hook import YotubeHook
from src.dados.ioperacoes_dados import IoperacaoDados
from typing import Dict, Optional


class YoutubeBuscaOperator(YoutubeOperator):

    def __init__(self,
                 dados_arquivo_json_salvar: IoperacaoDados,
                 dados_pkl_canal: IoperacaoDados,
                 operacao_hook: YotubeHook,
                 task_id,
                 assunto: str,
                 dados_pkl_canal_video: Optional[IoperacaoDados] = None,

                 **kwargs):
        super().__init__(
            dados_arquivo_json_salvar=dados_arquivo_json_salvar,
            dados_pkl_canal=dados_pkl_canal,
            dados_pkl_canal_video=dados_pkl_canal_video,
            operacao_hook=operacao_hook,
            assunto=assunto,

            task_id=task_id,
            **kwargs
        )

    def gravar_dados(self, req: Dict):
        req['assunto'] = self._assunto

        self._dados_arquivo_json_salvar_req.salvar_dados(dados=req)

        lista_canal_video = [
            (
                item['snippet']['channelId'],
                item['id']['videoId']
            )
            for item in req['items']
        ]

        lista_canais = [
            item['snippet']['channelId']
            for item in req['items']
        ]

        self._operacao_dados_pkl_canal.salvar_dados(dados=lista_canais)
        self._operacao_dados_pkl_canal_video.salvar_dados(
            dados=lista_canal_video)

    def execute(self, context):
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(req=json_response)
        except Exception as E:
            print(E)
            exit
