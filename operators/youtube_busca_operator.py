from operators.youtube_operator import YoutubeOperator
from hook.youtube_hook import YotubeHook
from src.dados.ioperacoes_dados import IoperacaoDados
from typing import Dict


class YoutubeBuscaOperator(YoutubeOperator):

    def __init__(self, operacao_hook: YotubeHook, task_id: str, dados_arquivo_json: IoperacaoDados, dados_pkl_canal_video: IoperacaoDados, dados_pkl_canal: IoperacaoDados, **kwargs):

        self.__operacao_dados_json_req = dados_arquivo_json
        self.__operacao_dados_pkl_canal_video = dados_pkl_canal_video
        self.__operacao_dados_pkl_canal = dados_pkl_canal

        super().__init__(operacao_hook=operacao_hook, task_id=task_id, **kwargs)

    def gravar_dados(self, req: Dict):
        self.__operacao_dados_json_req.salvar_dados(dados=req)
        lista_canal_video = [
            (
                item['snippet']['channelId'],
                item['id']['videoId']
            )
            for item in req['items']
        ]
        lista_canais = list(set(lista_canal_video[0]))
        self.__operacao_dados_pkl_canal.salvar_dados(dados=lista_canais)
        self.__operacao_dados_pkl_canal_video.salvar_dados(
            dados=lista_canal_video)

    def execute(self, context):
        for json_response in self._operacao_hook.rodar_dag():
            self.gravar_dados(req=json_response)
