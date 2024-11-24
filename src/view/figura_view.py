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
                barmode='group',
                title=f'Análise Total por Canal e Turno - {coluna_analise.capitalize()}',
                labels={
                    'dia_da_semana': 'Dia da Semana',
                    coluna_turno: 'Total por Turno',
                    'turno_extracao': 'Turno'
                }
            )
            st.plotly_chart(fig)
        else:
            st.write("Sem valores válidos na coluna.")

    def gerar_grafico_total_por_canal_dia(self, dataframe: pd.DataFrame, coluna_analise: str):
        dias_semana_ordenacao = ['Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira',
                                 'Quinta-feira', 'Sexta-feira', 'Sábado']

        dataframe['dia_da_semana'] = pd.Categorical(
            dataframe['dia_da_semana'], categories=dias_semana_ordenacao, ordered=True)
        fig = px.bar(dataframe, y=f'{coluna_analise}_dia',
                     x='dia_da_semana')
        fig.update_layout(
            yaxis=dict(
                categoryorder='array',
                categoryarray=dias_semana_ordenacao
            )
        )

        st.plotly_chart(fig)

    def gerar_grafico_video_visualizacoes(self, dataframe: pd.DataFrame, coluna_analise: str):
        ######
        st.dataframe(dataframe)

        coluna_turno = f'{coluna_analise}_turno'

        if coluna_turno not in dataframe.columns:
            st.write(f"A coluna '{coluna_turno}' não existe no DataFrame.")
            return
        dataframe = dataframe[dataframe[coluna_turno] > 0]

        dataframe['dia_semana'] = dataframe['dia_semana'].str.strip().str.title()

        dias_semana_ordenacao = [
            'Domingo', 'Segunda-Feira', 'Terça-Feira', 'Quarta-Feira',
            'Quinta-Feira', 'Sexta-Feira', 'Sábado'
        ]

        dataframe['dia_semana'] = pd.Categorical(
            dataframe['dia_semana'], categories=dias_semana_ordenacao, ordered=True
        )

        turno_ordem = ['Manhã', 'Tarde', 'Noite']
        dataframe['turno_extracao'] = pd.Categorical(
            dataframe['turno_extracao'], categories=turno_ordem, ordered=True
        )

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

    def gerar_grafico_taxa_engajamento_total_inscritos(self, dataframe: pd.DataFrame, coluna_analise=str):
        st.dataframe(dataframe)

        if coluna_analise not in dataframe.columns:
            st.write(f"A coluna '{coluna_analise}' não existe no DataFrame.")
            return
        dataframe = dataframe[dataframe[coluna_analise] > 0]

        dataframe['dia_da_semana'] = dataframe['dia_da_semana'].str.strip().str.title()

        dias_semana_ordenacao = [
            'Domingo', 'Segunda-Feira', 'Terça-Feira', 'Quarta-Feira',
            'Quinta-Feira', 'Sexta-Feira', 'Sábado'
        ]

        dataframe['dia_da_semana'] = pd.Categorical(
            dataframe['dia_da_semana'], categories=dias_semana_ordenacao, ordered=True
        )

        if dataframe.empty:
            st.write("O DataFrame está vazio.")
            return

        fig = px.bar(
            dataframe,
            x='dia_da_semana',
            y=coluna_analise,
            barmode='group',

        )
        st.plotly_chart(fig)
