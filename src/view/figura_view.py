import pandas as pd
import streamlit as st


class FiguraView:
    def gerar_grafico_total_por_canal_turno(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_total_por_canal_dia(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)
