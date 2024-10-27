from hook.youtube_hook import YotubeHook


class YoutubeBuscaAssuntoHook(YotubeHook):
    def __init__(self, conn_id, assunto_pesquisa: str, data_publicacao: str, carregar_dados=None):
        self.__assunto_pesquisa = assunto_pesquisa
        self.__data_publicacao = data_publicacao
        super().__init__(conn_id, carregar_dados)

    def _criar_url(self) -> str:
        """Retorna a url

        Returns:
            str: _description_
        """
        return self._URL + '/search/'

    def run(self):
        """Método para rodar a dag

        Returns:
            _type_: _description_
        """
        session = self.get_conn()

        url = self._criar_url()

        params = [
            {
                'part':  'snippet',
                'key': self._CHAVE,
                'regionCode': 'BR',
                'relevanceLanguage': 'pt',
                'maxResults': '50',
                'publishedAfter': self.__data_publicacao,
                'q': self.__assunto_pesquisa,
                'pageToken': ''
            }
        ]

        response = self._executar_paginacao(
            url=url, session=session, params=params)
        return response
