import streamlit as st
import pandas as pd
from src.controller.dashboard_controller import DashboardController


class DashboardView:

    st.set_page_config(layout='wide')

    def __init__(self):
        self.__controller = DashboardController()

    def gerar_layout_assunto(self):
        with st.container():
            menu_assunto = [
                'Linux',
                'Power BI',
                'Python',
                'cities skylines',
                'monster hunter'
            ]

            st.markdown("""
                <style>
                    .stRadio > div {
                        display: flex;
                        justify-content: center;
                    }
                    [data-testid="stRadio"] > div > div > div:first-child {
                        text-align: center;
                        width: 100%;
                        display: block;
                        margin: 0 auto;
                    }

                </style>

            """, unsafe_allow_html=True)

            assunto = st.radio('Escolha o assunto:',
                               menu_assunto, horizontal=True)

            return assunto

    def gerar_layout_analise_canais(self, assunto: str):
        with st.container():
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    """
                        <div style="text-align: center;">
                            <strong>Total visualizações/inscritos/vídeos publicados turno canal</strong>
                        </div>
                    """,
                    unsafe_allow_html=True
                )
                tab1, tab2, tab3 = st.tabs(
                    ['Total vísualizações', 'Total Inscritos', 'Total Vídeo']
                )
                with tab1:
                    coluna_analise = 'total_visualizacoes'
                    canais = self.__controller

    def rodar_dashboard(self):
        assunto = self.gerar_layout_assunto()
        self.gerar_layout_analise_canais(assunto=assunto)

    # def mostrar_input_canal(self, dataframe: pd.DataFrame, chave_input: int):
    #     return st.selectbox('Escolha o canal',
    #                         dataframe['nm_canal'].to_list(), key=chave_input)

    # def mostrar_input_canal_multiplos(self, dataframe: pd.DataFrame, chave_input: int):
    #     return st.multiselect('Escolha o canal',
    #                           dataframe['nm_canal'].to_list(), dataframe['nm_canal'].to_list()[0], key=chave_input)

    # def mostrar_input_videos_multiplos(self, dataframe: pd.DataFrame, chave_input: int):
    #     return st.multiselect('Escolha o canal',
    #                           dataframe['titulo_video'].to_list(), dataframe['titulo_video'].to_list()[0], key=chave_input)

    # def mostrar_input_video(self, dataframe: pd.DataFrame, chave_input: int):
    #     return st.selectbox('Escolha o video',
    #                         dataframe['titulo_video'].to_list(), key=chave_input)

    # def gerar_grafico_total_por_canal_turno(self, dataframe: pd.DataFrame):
    #     st.dataframe(dataframe)

    # def gerar_grafico_total_por_canal_dia(self, dataframe: pd.DataFrame):
    #     st.dataframe(dataframe)

    # def gerar_grafico_video_turno(self, dataframe: pd.DataFrame):
    #     st.dataframe(dataframe)

    # def gerar_grafico_engajamento_canal(self, dataframe: pd.DataFrame):
    #     st.dataframe(dataframe)

    # def gerar_grafico_engajamento_canal_total_incritos(self, dataframe: pd.DataFrame):
    #     st.dataframe(dataframe)
