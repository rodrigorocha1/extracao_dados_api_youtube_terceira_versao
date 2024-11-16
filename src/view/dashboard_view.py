import streamlit as st
import pandas as pd


class DashboardView:
    def mostrar_input_canal(self, dataframe: pd.DataFrame, chave_input: int):
        return st.selectbox('Escolha o canal',
                            dataframe['nm_canal'].to_list(), key=chave_input)

    def gerar_grafico_total_por_canal_turno(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)
