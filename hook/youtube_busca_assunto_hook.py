from hook.youtube_hook import YotubeHook
from typing import Dict, Generator


class YoutubeBuscaAssuntoHook(YotubeHook):
    def __init__(self, conn_id, assunto_pesquisa: str, data_publicacao: str, carregar_dados=None):
        self.__assunto_pesquisa = assunto_pesquisa
        self.__data_publicacao = data_publicacao
        super().__init__(conn_id, carregar_dados)

    def conectar_api(self, **kwargs) -> Dict:
        response = self._youtube.search().list(
            q=self.__assunto_pesquisa,
            type='video',
            part='snippet',
            maxResults=50,
            publishedAfter=self.__data_publicacao,
            pageToken=kwargs['pageToken']
        ).execute()
        return response

    def rodar_dag(self) -> Generator[Dict, None, None]:
        flag_paginacao = True
        page_token = ''
        while flag_paginacao:

            response = self.conectar_api(
                assunto='Transport Fever 2', pageToken=page_token)
            print(type(response))
            yield response

            flag_paginacao, page_token = self._executar_paginacao(
                response=response)
            print(flag_paginacao, page_token)
