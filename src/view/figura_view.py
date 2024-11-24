import pandas as pd
import streamlit as st
import plotly.express as px


class FiguraView:
    def __init__(self):
        self.__altura = 500
        self.__largura = 800
        self.__fonte_tamanho_hover_lavel = 14
        self.__fonte_tamanho_titulo = 14
        self.__cores_turno = {
            'Manhã': 'rgb(255, 223, 0)',  # Amarelo
            'Tarde': 'rgb(255, 87, 34)',   # Laranja
            'Noite': 'rgb(33, 150, 243)'   # Azul
        }

    def gerar_grafico_total_por_canal_turno(self, dataframe: pd.DataFrame, coluna_analise: str):
        st.dataframe(dataframe)

    def gerar_grafico_total_por_canal_dia(self, dataframe: pd.DataFrame, coluna_analise: str, cor_grafico: str):
        dias_semana_ordenacao = ['Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira',
                                 'Quinta-feira', 'Sexta-feira', 'Sábado']

        dataframe['dia_da_semana'] = pd.Categorical(
            dataframe['dia_da_semana'], categories=dias_semana_ordenacao, ordered=True)
        fig = px.bar(
            dataframe,
            y=f'{coluna_analise}_dia',
            x='dia_da_semana',
            text=f'{coluna_analise}_dia',
            title=f'Análise Total por Canal e Dia - {coluna_analise.capitalize()}',
        )

        fig.update_traces(
            textposition='outside',
            hovertemplate=(

                "<b>Dia da Semana:</b> %{x}<br>"
                "<b>Total:</b> %{y}<br>"
                "<extra></extra>"
            ),
            marker=dict(color=cor_grafico),
            textfont=dict(
                size=self.__fonte_tamanho_titulo
            ),
        )
        fig.update_layout(
            xaxis_title='Dias da Semana',
            yaxis_title='Total por Canal',
            yaxis=dict(
                categoryorder='array',
                categoryarray=dias_semana_ordenacao
            ),
            bargap=0.6,
            width=self.__largura,
            height=self.__altura,
            hoverlabel=dict(
                font_size=self.__fonte_tamanho_hover_lavel,
                font_family="Arial"
            )
        )
        st.plotly_chart(fig)

    def gerar_grafico_video_visualizacoes(self, dataframe: pd.DataFrame, coluna_analise: str):
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
            text=coluna_turno,
            category_orders={'turno_extracao': turno_ordem},
            color_discrete_map=self.__cores_turno,
            custom_data=['turno_extracao'],
            title=f'Análise dados Vídeo {coluna_turno.replace("_", " ").capitalize()}'
        )

        hover_template = (
            "<b>Dia da Semana:</b> %{x}<br>"
            "<b>Turno:</b> %{customdata[0]}<br>"
            "<b>Visualizações:</b> %{y}<br>"
            "<extra></extra>"
        )

        fig.update_traces(
            hovertemplate=hover_template,
            textposition='outside'
        )

        fig.update_layout(
            xaxis_title='Dias da Semana',
            yaxis_title=coluna_analise,
            yaxis=dict(
                categoryorder='array',
                categoryarray=dias_semana_ordenacao
            ),
            bargap=0.1,
            width=self.__largura + 350,
            height=self.__altura,
            hoverlabel=dict(
                font_size=self.__fonte_tamanho_hover_lavel,
                font_family="Arial"
            ),
            legend=dict(
                title='Turno de Extração',
                font=dict(
                    family="Arial",
                    size=12
                ),
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.05
            ),
            margin=dict(
                l=300,  # Margem esquerda
                # r=50,  # Margem direita
                # t=50,  # Margem superior
                # b=50   # Margem inferior
            )
        )

        st.plotly_chart(fig)

    def gerar_grafico_taxa_engajamento_total_inscritos(self, dataframe: pd.DataFrame, coluna_analise: str, cor_grafico: str):

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
            text=coluna_analise,
            title='Análise taxa engajamento',
            color='nm_canal'

        )
        hover_template = (
            "<b>Dia da Semana:</b> %{x}<br>"
            "<b>Média taxa engajamento:</b> %{y}<br>"
            "<extra></extra>"
        )
        fig.update_traces(
            hovertemplate=hover_template,
            textposition='outside',
            # marker=dict(color=cor_grafico),
        )

        fig.update_layout(
            xaxis_title='Dias da Semana',
            yaxis_title=coluna_analise.replace('_', '').capitalize(),
            bargap=0.5,
            width=self.__largura + 350,
            height=self.__altura,

            hoverlabel=dict(
                font_size=self.__fonte_tamanho_hover_lavel,
                font_family="Arial"
            ),
            legend=dict(
                title='Nome Canal',
                font=dict(
                    family="Arial",
                    size=12
                ),
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.05
            ),
            margin=dict(
                l=300,  # Margem esquerda
                # r=50,  # Margem direita
                # t=50,  # Margem superior
                # b=50   # Margem inferior
            )
        )

        st.plotly_chart(fig)
