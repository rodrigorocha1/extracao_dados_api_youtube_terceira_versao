from src.model.medidas_model import Medida
from src.view.dashboard_view import DashboardView
import streamlit as st


class DashboardController:

    def __init__(self):
        self.__model = Medida()
        self.__view = DashboardView()

    def listar_canais_assunto(self, assunto: str):
        dataframe = self.__model.obter_depara_canal(assunto=assunto)
        self.__view.mostrar_usuarios(dataframe)
