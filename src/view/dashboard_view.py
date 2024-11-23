import streamlit as st
import pandas as pd
from src.controller.dashboard_controller import DashboardController
from src.view.figura_view import FiguraView


class DashboardView:

    st.set_page_config(layout='wide', page_title='Dashboard Youtube')

    def __init__(self):
        self.__controller = DashboardController()
        self.__figura_view = FiguraView()

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
                    input_canais = self.__controller.listar_canais_assunto(
                        assunto=assunto)
                    canais = st.selectbox(
                        'selecione o canal',
                        input_canais,
                        placeholder='selecione os canais',
                        key=1
                    )

                    dataframe = self.__controller.gerar_resultado_total_canais(
                        assunto=assunto, coluna_analise=coluna_analise, nome_canal=canais)

                    self.__figura_view.gerar_grafico_total_por_canal_turno(
                        dataframe=dataframe, coluna_analise=coluna_analise)
                with tab2:
                    st.write('Total Inscritos')
                    coluna_analise = 'total_inscritos'
                    input_canais = self.__controller.listar_canais_assunto(
                        assunto=assunto)
                    canais = st.selectbox(
                        'selecione o canal',
                        input_canais,
                        placeholder='selecione os canais',
                        key=2
                    )

                    dataframe = self.__controller.gerar_resultado_total_canais(
                        assunto=assunto, coluna_analise=coluna_analise, nome_canal=canais)

                    self.__figura_view.gerar_grafico_total_por_canal_turno(
                        dataframe=dataframe, coluna_analise=coluna_analise)
                with tab3:
                    st.write('Total Vídeo')
                    coluna_analise = 'total_videos_publicados'
                    input_canais = self.__controller.listar_canais_assunto(
                        assunto=assunto)
                    canais = st.selectbox(
                        'selecione o canal',
                        input_canais,
                        placeholder='selecione os canais',
                        key=3
                    )
                    dataframe = self.__controller.gerar_resultado_total_canais(
                        assunto=assunto, coluna_analise=coluna_analise, nome_canal=canais)
                    self.__figura_view.gerar_grafico_total_por_canal_turno(
                        dataframe=dataframe, coluna_analise=coluna_analise)

            with col2:

                st.markdown(
                    """
                        <div style="text-align: center;">
                            <strong>Total vísualizações/inscritos/videos_publicado por dia canal</strong>
                        </div>
                    """,
                    unsafe_allow_html=True
                )

                tab1, tab2, tab3 = st.tabs(
                    ['Total vísualizações', 'Total Inscritos', 'Total Vídeo']
                )
                with tab1:
                    coluna_analise = 'total_visualizacoes'
                    input_canais = self.__controller.listar_canais_assunto(
                        assunto=assunto)
                    canais = st.selectbox(
                        'selecione o canal',
                        input_canais,
                        placeholder='selecione os canais',
                        key=4
                    )
                    dataframe = self.__controller.gerar_resultado_total_canais(
                        coluna_analise=coluna_analise, assunto=assunto, nome_canal=canais, flag_turno=2)
                    self.__figura_view.gerar_grafico_total_por_canal_dia(
                        dataframe=dataframe, coluna_analise=coluna_analise)

                with tab2:
                    coluna_analise = 'total_inscritos'
                    input_canais = self.__controller.listar_canais_assunto(
                        assunto=assunto)
                    canais = st.selectbox(
                        'selecione o canal',
                        input_canais,
                        placeholder='selecione os canais',
                        key=5
                    )
                    dataframe = self.__controller.gerar_resultado_total_canais(
                        coluna_analise=coluna_analise, assunto=assunto, nome_canal=canais, flag_turno=2)
                    self.__figura_view.gerar_grafico_total_por_canal_dia(
                        dataframe=dataframe, coluna_analise=coluna_analise)
                with tab3:
                    coluna_analise = 'total_videos_publicados'
                    input_canais = self.__controller.listar_canais_assunto(
                        assunto=assunto)
                    canais = st.selectbox(
                        'selecione o canal',
                        input_canais,
                        placeholder='selecione os canais',
                        key=6
                    )
                    dataframe = self.__controller.gerar_resultado_total_canais(
                        coluna_analise=coluna_analise, assunto=assunto, nome_canal=canais, flag_turno=2)
                    self.__figura_view.gerar_grafico_total_por_canal_dia(
                        dataframe=dataframe, coluna_analise=coluna_analise)

    def gerar_layout_analise_video(self, assunto: str):
        st.write('Análise vídeo')

        tab1, tab2, tab3 = st.tabs(
            ['Total vísualizações', 'Total Likes', 'Total Comentários']
        )

        with st.container():

            with tab1:
                st.write('Total vísualizações')
                coluna_analise = 'total_visualizacoes'

                input_canais = self.__controller.listar_canais_assunto(
                    assunto=assunto)

                canais = st.selectbox(
                    'selecione o canal',
                    input_canais,
                    placeholder='selecione os canais',
                    key=7
                )
                titulo_video = self.__controller.listar_inputs_canal_video_assunto(
                    assunto=assunto, nome_canal=canais)

                videos = st.selectbox(
                    'Escolha o vídeo ',
                    titulo_video,
                    placeholder='selecione o vídeo',
                    key=8
                )
                dataframe = self.__controller.gerar_resultados_videos(
                    assunto=assunto, titulo_video=videos, coluna_analise=coluna_analise)
                self.__figura_view.gerar_grafico_video_visualizacoes(
                    dataframe=dataframe)
            with tab2:
                coluna_analise = 'total_likes'
                input_canais = self.__controller.listar_canais_assunto(
                    assunto=assunto)
                canais = st.selectbox(
                    'selecione o canal',
                    input_canais,
                    placeholder='selecione os canais',
                    key=9
                )
                titulo_video = self.__controller.listar_inputs_canal_video_assunto(
                    assunto=assunto, nome_canal=canais)
                videos = st.selectbox(
                    'Escolha o vídeo ',
                    titulo_video,
                    placeholder='selecione o vídeo',
                    key=10
                )
                dataframe = self.__controller.gerar_resultados_videos(
                    assunto=assunto, titulo_video=videos, coluna_analise=coluna_analise)
                self.__figura_view.gerar_grafico_video_likes(
                    dataframe=dataframe)
            with tab3:
                coluna_analise = 'total_comentarios'
                input_canais = self.__controller.listar_canais_assunto(
                    assunto=assunto)
                canais = st.selectbox(
                    'selecione o canal',
                    input_canais,
                    placeholder='selecione os canais',
                    key=11
                )
                titulo_video = self.__controller.listar_inputs_canal_video_assunto(
                    assunto=assunto, nome_canal=canais)
                videos = st.selectbox(
                    'Escolha o vídeo ',
                    titulo_video,
                    placeholder='selecione o vídeo',
                    key=12
                )
                dataframe = self.__controller.gerar_resultados_videos(
                    assunto=assunto, titulo_video=videos, coluna_analise=coluna_analise)
                self.__figura_view.gerar_grafico_video_comentarios(
                    dataframe=dataframe)

    def gerar_layout_taxa_engajamento(self, assunto: str):
        with st.container():
            st.write('Análise Taxa engajamento')

            tab1, tab2 = st.tabs(
                [
                    'Média engajamento canal vísualização',
                    'Média engajamento total inscritos'
                ]
            )
            with tab1:
                st.write('Média engajamento  do  canal por visualização')
                lista_canais = self.__controller.gerar_canal_input_multiplos(
                    assunto=assunto)
                nome_canais = st.multiselect(
                    'Escolha um ou mais canais',
                    lista_canais,
                    lista_canais[0],
                    key=13
                )

                dataframe = self.__controller.gerar_layout_total_engagamento_canais(
                    assunto=assunto, nome_canal=nome_canais)
                self.__figura_view.gerar_grafico_taxa_engajamento_total_inscritos(
                    dataframe=dataframe)
            with tab2:
                st.write('Média engajamento do total por inscritos')
                lista_canais = self.__controller.gerar_canal_input_multiplos(
                    assunto=assunto)
                nome_canais = st.multiselect(
                    'Escolha um ou mais canais',
                    lista_canais,
                    lista_canais[0],
                    key=14
                )
                dataframe = self.__controller.gerar_layout_total_engajamento_inscritos(
                    assunto=assunto, nome_canal=nome_canais)
                self.__figura_view.gerar_grafico_taxa_engajamento_total_inscritos(
                    dataframe=dataframe)

    def rodar_dashboard(self):
        assunto = self.gerar_layout_assunto()
        self.gerar_layout_analise_canais(assunto=assunto)
        self.gerar_layout_analise_video(assunto=assunto)
        self.gerar_layout_taxa_engajamento(assunto=assunto)
