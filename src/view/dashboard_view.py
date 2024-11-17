import streamlit as st
import pandas as pd


class DashboardView:

    def mostrar_input_canal(self, dataframe: pd.DataFrame, chave_input: int):
        return st.selectbox('Escolha o canal',
                            dataframe['nm_canal'].to_list(), key=chave_input)

    def mostrar_input_canal_multiplos(self, dataframe: pd.DataFrame, chave_input: int):
        return st.multiselect('Escolha o canal',
                              dataframe['nm_canal'].to_list(), dataframe['nm_canal'].to_list()[0], key=chave_input)

    def mostrar_input_video(self, dataframe: pd.DataFrame, chave_input: int):
        return st.selectbox('Escolha o video',
                            dataframe['titulo_video'].to_list(), key=chave_input)

    def gerar_grafico_total_por_canal_turno(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_total_por_canal_dia(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_video_turno(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_engajamento_canal(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_engajamento_canal_total_incritos(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)
