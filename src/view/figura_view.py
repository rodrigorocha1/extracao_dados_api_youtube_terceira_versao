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
            turno_order = ['Manhã', 'Tarde', 'Noite']

            dataframe['turno_extracao'] = pd.Categorical(
                dataframe['turno_extracao'], categories=turno_order, ordered=True)
            if dataframe.empty:
                st.write("O DataFrame está vazio.")
                return
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

    def gerar_grafico_total_por_canal_dia(self, dataframe: pd.DataFrame, coluna_analise: str):
        dias_semana_ordenacao = ['Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira',
                                 'Quinta-feira', 'Sexta-feira', 'Sábado', ]

        dataframe['dia_da_semana'] = pd.Categorical(
            dataframe['dia_da_semana'], categories=dias_semana_ordenacao, ordered=True)
        st.dataframe(dataframe)
        fig = px.bar(dataframe, x=f'{coluna_analise}_dia',
                     y='dia_da_semana', orientation='h')
        fig.update_layout(
            yaxis=dict(
                categoryorder='array',
                categoryarray=dias_semana_ordenacao
            )
        )

        st.plotly_chart(fig)

    def gerar_grafico_video_comentarios(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_video_visualizacoes(self, dataframe: pd.DataFrame, coluna_analise: str):
        ######
        st.dataframe(dataframe)
        coluna_turno = f'{coluna_analise}_turno'

        if coluna_turno not in dataframe.columns:
            st.write(f"A coluna '{coluna_turno}' não existe no DataFrame.")
            return

        if not (dataframe[coluna_turno] == 0).all():
            turno_ordem = ['Manhã', 'Tarde', 'Noite']
            dias_semana_ordenacao = ['Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira',
                                     'Quinta-feira', 'Sexta-feira', 'Sábado', ]

            # dataframe['dia_semana'] = pd.Categorical(
            #     dataframe['dia_semana'], categories=dias_semana_ordenacao, ordered=True)

            dataframe['turno_extracao'] = pd.Categorical(
                dataframe['turno_extracao'], categories=turno_ordem, ordered=True)
            if dataframe.empty:
                st.write("O DataFrame está vazio.")
                return
            fig = px.bar(
                dataframe,
                x='dia_semana',
                y=coluna_turno,
                color='turno_extracao',
                barmode='group',
                category_orders={'turno_extracao': turno_ordem},
            )
            st.plotly_chart(fig)
        else:
            st.write("Sem valores válidos na coluna.")

    def gerar_grafico_taxa_engajamento_visualizacao(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)

    def gerar_grafico_taxa_engajamento_total_inscritos(self, dataframe: pd.DataFrame):
        st.dataframe(dataframe)
