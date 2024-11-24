from src.model.medidas_model import Medida
from typing import Tuple, List
import pandas as pd


class DashboardController:

    def __init__(self):
        self.__model = Medida()

    def listar_canais_assunto(self,  assunto: str, flag_input_canal: int = 1) -> Tuple[str]:
        """Método para listar canais

        Args:
            assunto (str): assunto
            flag_input_canal (int, optional): Flag para trocar o input canal  1 - Exibir  id_canal - 2 exibir nome canal. Defaults to 1. 

        Returns:
            Tuple[str]: Tupla com id canal ou com o nome canal
        """

        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        if flag_input_canal == 1:
            dados_canal = tuple(dataframe['id_canal'].tolist())
        else:
            dados_canal = tuple(dataframe['nm_canal'].to_list())

        return dados_canal

    def gerar_resultado_total_canais(self, assunto: str, coluna_analise: str, dado_canal: str, flag_turno: int = 1,  flag_input_canal: int = 1) -> pd.DataFrame:
        """Método para gerar restultado canais turno

        Args:
            assunto (str): assunto de pesquisa
            coluna_analise (str): [total_visualizacoes, total_inscritos, total_videos_publicados]
            dado_canal (str): pode ser o nome do canal ou id canal
            flag_turno (int, optional): Direcionameto para consultar dados por turno ou por dia: 1-Turno/2-Dia. Defaults to 1.
            flag_input_canal (int, optional): Flag para trocar o input canal  1 - Exibir  id_canal - 2 exibir nome canal. Defaults to 1.
        Returns:
            pd.DataFrame: _description_
        """
        if flag_input_canal == 2:

            id_canal = self.__model.obter_depara_canal(
                assunto=assunto, flag=2, nm_canal=dado_canal)

            id_canal = id_canal.to_string().split(' ')[-1]
        else:
            id_canal = dado_canal
        if flag_turno == 1:
            dataframe = self.__model.obter_dados_canal_turno(
                assunto=assunto, id_canal=id_canal, coluna_analise=coluna_analise)
        else:
            dataframe = self.__model.obter_dado_canal_dia(
                assunto=assunto, id_canal=id_canal, coluna_analise=coluna_analise)
        return dataframe

    def gerar_canal_input_multiplos(self, assunto: str, flag_input: int) -> List[str]:
        """Método para gerar lista de canais para input

        Args:
            assunto (str): assuto de pesquisa

        Returns:
            List[str]: Lista com os nomes dos canais
        """
        lista_canais = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        if flag_input == 1:
            lista_canais = lista_canais['id_canal'].to_list()
        else:
            lista_canais = lista_canais['nm_canal'].to_list()
        return lista_canais

    def listar_video_assunto(self, assunto: str) -> Tuple[str]:
        """Método para  recuperar assunto

        Args:
            assunto (str): assunto de pesquisa

        Returns:
            Tuple[str]: tupla com os assuntos
        """
        dataframe = self.__model.obter_depara_video(
            assunto=assunto, flag=1, titulo_video=None, id_canal=None)
        canais = tuple(dataframe['titulo_video'].tolist())
        return canais

    def listar_canal_video_assunto(self, assunto: str, nome_canal: str) -> Tuple[str]:
        dataframe = self.__model.obter_depara_video(
            assunto=assunto, flag=1, titulo_video=None, id_canal=None)
        canais = tuple(dataframe['titulo_video'].tolist())
        return canais

    def gerar_resultados_videos(self, assunto: str, titulo_video: str, coluna_analise: str, flag_input: int) -> pd.DataFrame:
        if flag_input == 1:
            id_video = titulo_video
        else:
            id_video = self.__model.obter_depara_video(
                assunto=assunto, flag=2, titulo_video=titulo_video, id_canal=None)
            id_video = id_video['id_video'].to_string().split(' ')[-1]
        dataframe = self.__model.obter_total_dados_video_turno(
            assunto=assunto, coluna_analise=coluna_analise, id_video=id_video)
        return dataframe

    def listar_inputs_canal_video_assunto(self, nome_canal: str, assunto: str, flag_input_canal: int) -> Tuple[str]:
        print('flag_input_canal', flag_input_canal)
        if flag_input_canal == 1:
            id_canal = nome_canal
            dataframe = self.__model.obter_depara_video(
                assunto=assunto, flag=5, titulo_video=None, id_canal=id_canal)
            titulos_video = tuple(dataframe['id_video'].to_list())
        else:
            id_canal = self.__model.obter_depara_canal(
                assunto=assunto, flag=2, nm_canal=nome_canal)
            id_canal = id_canal.to_string().split(' ')[-1]
            dataframe = self.__model.obter_depara_video(
                assunto=assunto, flag=3, titulo_video=None, id_canal=id_canal)
            titulos_video = tuple(dataframe['titulo_video'].to_list())

        return titulos_video

    def gerar_layout_total_engagamento_canais(self, assunto: str, nome_canal: List[str], flag_input: int) -> pd.DataFrame:
        if flag_input == 1:
            lista_id_canais = nome_canal
        else:
            lista_id_canais = self.__model.obter_depara_canal(
                assunto=assunto, flag=2, nm_canal=nome_canal)
            lista_id_canais = lista_id_canais['id_canal'].to_list()

        dataframe = self.__model.obter_media_engajamento_canal_visualizacoes(
            assunto=assunto, ids_canal=lista_id_canais)
        return dataframe

    def gerar_layout_total_engajamento_inscritos(self, assunto: str, nome_canal: List[str], flag_input: int):
        if flag_input == 1:
            lista_id_canais = nome_canal
        else:
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
