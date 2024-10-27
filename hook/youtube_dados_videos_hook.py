
from hook.youtube_hook import YotubeHook
from src.dados.ioperacoes_dados import IoperacaoDados


class YoutubeBuscaVideoHook(YotubeHook):
    def __init__(self,  carregar_canais_brasileiros: IoperacaoDados, carregar_dados=None,  conn_id=None,):
        self.__carregar_canais_brasileiros = carregar_canais_brasileiros
        super().__init__(conn_id, carregar_dados)

    def _criar_url(self):
        return self._URL + '/videos/'

    def run(self):
        session = self.get_conn()
        lista_canais_brasileiros = self.__carregar_canais_brasileiros.carregar_dados()
        lista_canais_video = self._carregar_dados.carregar_dados()
        url = self._criar_url()
        params = []
        for canal_video in lista_canais_video:
            if canal_video[1] in lista_canais_brasileiros:
                param = {
                    'part':  'statistics,contentDetails,id,snippet,status',
                    'id': canal_video[0],
                    'key': self._CHAVE,
                    'regionCode': 'BR',
                    'pageToken': ''

                }
                params.append(param)

        response = self._executar_paginacao(
            url=url,
            session=session,
            params=params
        )
        return response
