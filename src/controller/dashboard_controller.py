from src.model.medidas_model import Medida
from src.view.dashboard_view import DashboardView
import streamlit as st


class DashboardController:

    def __init__(self):
        self.__model = Medida()
        self.__view = DashboardView()

    def listar_canais_assunto(self, assunto: str, chave_input: int):
        dataframe = self.__model.obter_depara_canal(assunto=assunto, flag=1)
        canais = self.__view.mostrar_input_canal(
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
        print(dataframe)
        self.__view.gerar_grafico_total_por_canal_dia(dataframe=dataframe)
