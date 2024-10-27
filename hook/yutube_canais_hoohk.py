from hook.youtube_hook import YotubeHook
from src.dados.ioperacoes_dados import IoperacaoDados


class YoutubeBuscaCanaisHook(YotubeHook):
    def __init__(self, conn_id: str = None, carregar_dados: IoperacaoDados = None) -> None:
        super().__init__(conn_id, carregar_dados)

    def _criar_url(self):
        return self._URL + '/channels/'

    def run(self):
        session = self.get_conn()
        lista_canais = self._carregar_dados.carregar_dados()

        url = self._criar_url()
        params = [
            {
                'part': 'snippet,statistics',
                'id': id_canal,
                'key': self._CHAVE,

            } for id_canal in lista_canais
        ]
        response = self._executar_paginacao(
            url=url,
            session=session,
            params=params
        )
        return response
