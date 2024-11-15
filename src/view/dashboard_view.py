import streamlit as st
import pandas as pd


class DashboardView:
    def mostrar_input_canal(self, dataframe: pd.DataFrame, chave_input: int):
        return st.selectbox('Escolha o canal',
                            dataframe['nm_canal'].to_list(), key=chave_input)
