from src.model.medidas_model import Medida
from src.view.dashboard_view import DashboardView


class DashboardController:

    def __init__(self):
        self.__model = Medida()
        self.__view = DashboardView()

    def listar_video_assunto(self, assunto: str, chave_input: int):
        dataframe = self.__model.obter_depara_video(
            assunto=assunto, flag=1, titulo_video=None)
        canais = self.__view.mostrar_input_video(
            chave_input=chave_input, dataframe=dataframe
        )
        return canais

    def listar_canais_assunto(self, assunto: str, chave_input: int):
        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        canais = self.__view.mostrar_input_canal(
            dataframe, chave_input=chave_input,)
        return canais

    def listar_canais_assunto_multiplos(self, assunto: str, chave_input: int):
        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        canais = self.__view.mostrar_input_canal_multiplos(
            dataframe, chave_input=chave_input,)
        return canais

    def gerar_dados_total_por_canal_turno(self, canal: str, assunto: str,
                                          flag: int, coluna_analise: str):

        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=flag, nm_canal=canal)
        id_canal = dataframe['id_canal'].to_string().split(' ')[-1].strip()
        dataframe = self.__model.obter_dados_canal_turno(
            assunto=assunto, coluna_analise=coluna_analise, id_canal=id_canal)
        self.__view.gerar_grafico_total_por_canal_turno(dataframe=dataframe)

    def gerar_dados_canal_dia(self, canal: str, assunto: str,
                              flag: int, coluna_analise: str):
        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=flag, nm_canal=canal)
        id_canal = dataframe['id_canal'].to_string().split(' ')[-1].strip()
        dataframe = self.__model.obter_dado_canal_dia(
            assunto=assunto, id_canal=id_canal, coluna_analise=coluna_analise)

        self.__view.gerar_grafico_total_por_canal_dia(dataframe=dataframe)

    def gerar_dados_videos(self, video: str, assunto: str, flag: int, coluna_analise: str):
        dataframe = self.__model.obter_depara_video(
            assunto=assunto, flag=flag, titulo_video=video)
        id_video = dataframe['id_video'].to_string().split(
            ' ')[-1].strip()

        dataframe = self.__model.obter_total_dados_video_turno(
            assunto=assunto, id_video=id_video, coluna_analise=coluna_analise)

        self.__view.gerar_grafico_video_turno(dataframe=dataframe)

    def gerar_dados_engajamento_canal_visualizacao(self, assunto: str, flag: int, canal: list):

        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=flag, nm_canal=canal)
        print('Gerando datafame id canal')

        ids_canal = dataframe['id_canal'].tolist()

        dataframe = self.__model.obter_media_engajamento_canal(
            ids_canal=ids_canal, assunto=assunto)

        self.__view.gerar_grafico_engajamento_canal(dataframe)

    def gerar_dados_engajamneto_canal_total_inscritos(self, assunto: str, flag: int, canal: str):
        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=flag, canal=canal
        )
        ids_canal = dataframe['id_canal'].tolist()

    def gerar_dados_engajamento_video(self):
        pass
