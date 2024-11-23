import pandas as pd
import streamlit as st
import plotly.express as px


class FiguraView:
    def gerar_grafico_total_por_canal_turno(self, dataframe: pd.DataFrame, coluna_analise: str):
        st.dataframe(dataframe)

        coluna_turno = f'{coluna_analise}_turno'

        if coluna_turno not in dataframe.columns:
            st.write(f"A coluna '{coluna_turno}' não existe no DataFrame.")
            return

        if not (dataframe[coluna_turno] == 0).all():
            if dataframe.empty:
                st.write("O DataFrame está vazio.")
                return

            # Gerar o gráfico
            fig = px.bar(
                dataframe,
                x='dia_da_semana',
                y=coluna_turno,
                color='turno_extracao',
                barmode='group'
            )
            st.plotly_chart(fig)
        else:
            st.write("Sem valores válidos na coluna.")

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
