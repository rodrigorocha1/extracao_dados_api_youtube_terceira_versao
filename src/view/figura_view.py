import pandas as pd
import streamlit as st


class FiguraView:
    def gerar_grafico_total_por_canal_turno(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_total_por_canal_dia(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_video_likes(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_video_comentarios(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_video_visualizacoes(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_taxa_engajamento_visualizacao(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_taxa_engajamento_total_inscritos(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)
