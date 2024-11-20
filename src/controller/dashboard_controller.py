from src.model.medidas_model import Medida
from typing import Tuple, List
import pandas as pd


class DashboardController:

    def __init__(self):
        self.__model = Medida()

    def listar_canais_assunto(self,  assunto: str) -> Tuple[str]:
        """Método para listar canais

        Args:
            assunto (str): assunto

        Returns:
            Tuple[str]: tupla com os nomes dos canais
        """
        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        dados_canal = tuple(dataframe['nm_canal'].to_list())
        return dados_canal

    def gerar_resultado_total_canais(self, assunto: str, coluna_analise: str, nome_canal: str, flag_turno: int = 1) -> pd.DataFrame:
        """Método para gerar restultado canais turno

        Args:
            assunto (str): assunto de pesquisa
            coluna_analise (str): [total_visualizacoes, total_inscritos, total_videos_publicados]
            nome_canal (str): nome do canal
            flag_turno (int, optional): Direcionameto para consultar dados por turno ou por dia: 1-Turno/2-Dia. Defaults to 1.

        Returns:
            pd.DataFrame: _description_
        """
        id_canal = self.__model.obter_depara_canal(
            assunto=assunto, flag=2, nm_canal=nome_canal)
        id_canal = id_canal.to_string().split(' ')[-1]
        if flag_turno == 1:
            dataframe = self.__model.obter_dados_canal_turno(
                assunto=assunto, id_canal=id_canal, coluna_analise=coluna_analise)
        else:
            dataframe = self.__model.obter_dado_canal_dia(
                assunto=assunto, id_canal=id_canal, coluna_analise=coluna_analise)
        return dataframe

    def gerar_canal_input_multiplos(self, assunto: str) -> List[str]:
        """Método para gerar lista de canais para input

        Args:
            assunto (str): assuto de pesquisa

        Returns:
            List[str]: Lista com os nomes dos canais
        """
        lista_canais = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        lista_canais = lista_canais['nm_canal'].to_list()
        return lista_canais

    def listar_video_assunto(self, assunto: str) -> Tuple[str]:
        dataframe = self.__model.obter_depara_video(
            assunto=assunto, flag=1, titulo_video=None, id_canal=None)
        canais = tuple(dataframe['titulo_video'].tolist())
        return canais

    def listar_canal_video_assunto(self, assunto: str, nome_canal: str) -> Tuple[str]:
        dataframe = self.__model.obter_depara_video(
            assunto=assunto, flag=1, titulo_video=None, id_canal=None)
        canais = tuple(dataframe['titulo_video'].tolist())
        return canais

    def gerar_resultados_videos(self, assunto: str, titulo_video: str, coluna_analise: str) -> pd.DataFrame:
        id_video = self.__model.obter_depara_video(
            assunto=assunto, flag=2, titulo_video=titulo_video, id_canal=None)
        id_video = id_video['id_video'].to_string().split(' ')[-1]
        dataframe = self.__model.obter_total_dados_video_turno(
            assunto=assunto, coluna_analise=coluna_analise, id_video=id_video)
        return dataframe

    def listar_inputs_canal_video_assunto(self, nome_canal: str, assunto: str) -> Tuple[str]:
        id_canal = self.__model.obter_depara_canal(
            assunto=assunto, flag=2, nm_canal=nome_canal)
        id_canal = id_canal.to_string().split(' ')[-1]

        dataframe = self.__model.obter_depara_video(
            assunto=assunto, flag=3, titulo_video=None, id_canal=id_canal)
        titulos_video = tuple(dataframe['titulo_video'].to_list())
        return titulos_video

    def gerar_layout_total_engagamento_canais(self, assunto: str, nome_canal: List[str]) -> pd.DataFrame:
        lista_id_canais = self.__model.obter_depara_canal(
            assunto=assunto, flag=2, nm_canal=nome_canal)
        lista_id_canais = lista_id_canais['id_canal'].to_list()

        dataframe = self.__model.obter_media_engajamento_canal_visualizacoes(
            assunto=assunto, ids_canal=lista_id_canais)
        return dataframe

    def gerar_layout_total_engajamento_inscritos(self, assunto: str, nome_canal: List[str]):
        lista_id_canais = self.__model.obter_depara_canal(
            assunto=assunto, flag=2, nm_canal=nome_canal)
        lista_id_canais = lista_id_canais['id_canal'].to_list()
        dataframe = self.__model.obter_media_taxa_engajamento_canal_total_inscritos(
            assunto=assunto,
            ids_canal=lista_id_canais
        )
        return dataframe

    def gerar_inputs_multiplos_videos(self, nome_canal: List[str], assunto: str):
        lista_id_canais = self.__model.obter_depara_canal(
            assunto=assunto, flag=2, nm_canal=nome_canal)
        id_canal = lista_id_canais['id_canal'].to_list()

        lista_videos = self.__model.obter_depara_video(
            assunto=assunto, flag=3, titulo_video=None, id_canal=id_canal)
        lista_videos = lista_videos['titulo_video'].tolist()

        return lista_videos

    def gerar_resultado_engajamento_videos(self, assunto: str, lista_videos: List[str]) -> pd.DataFrame:
        dataframe_id_video = self.__model.obter_depara_video(
            assunto=assunto, flag=4, titulo_video=lista_videos)
        lista_id_video = dataframe_id_video['id_video'].to_list()
        dataframe_resultado = self.__model.obter_taxa_engajamento_video(
            assunto=assunto, id_video=lista_id_video)

        return dataframe_resultado
