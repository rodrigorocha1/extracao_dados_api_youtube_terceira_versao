from src.model.medidas_model import Medida
from src.view.dashboard_view import DashboardView
import streamlit as st


class DashboardController:

    def __init__(self):
        self.__model = Medida()
        self.__view = DashboardView()

    def listar_canais_assunto(self, assunto: str, chave_input: int):
        dataframe = self.__model.obter_depara_canal(assunto=assunto)
        canais = self.__view.mostrar_input_canal(
            dataframe, chave_input=chave_input)
        return canais
