from operators.youtube_operator import YoutubeOperator


class YoutubeBuscaOperator(YoutubeOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        for json_response in self._operacao_hook.rodar_dag():
            self._operacao_dados.salvar_dados(dados=json_response)
            lista_canal_video = [
                (
                    item['snippet']['channelId'],
                    item['id']['videoId']
                )
                for item in json_response['items']
            ]
            lista_canais = list(set(lista_canal_video[0]))
            self._operacao_dados.salvar_dados(dados=lista_canais)
            self._operacao_dados.salvar_dados(dados=lista_canal_video)
