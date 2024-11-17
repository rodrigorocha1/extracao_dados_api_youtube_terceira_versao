import streamlit as st
from src.controller.dashboard_controller import DashboardController


def main():
    st.set_page_config(layout='wide')
    st.title('Dashboard Youtube')

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

        dc = DashboardController()
        assunto = st.radio('Escolha o assunto:', menu_assunto, horizontal=True)

        st.write(assunto)

    with st.container():
        st.write('Análse canais')

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
                canal_um = dc.listar_canais_assunto(
                    assunto=assunto, chave_input=1)
                dc.gerar_dados_total_por_canal_turno(
                    canal=canal_um, assunto=assunto, flag=2, coluna_analise=coluna_analise)

            with tab2:
                st.write('Total Inscritos')
                coluna_analise = 'total_inscritos'
                canal_dois = dc.listar_canais_assunto(
                    assunto=assunto, chave_input=2)
                dc.gerar_dados_total_por_canal_turno(
                    canal=canal_dois, assunto=assunto, flag=2, coluna_analise=coluna_analise)

            with tab3:
                st.write('Total Vídeo')
                coluna_analise = 'total_videos_publicados'
                canal_tres = dc.listar_canais_assunto(
                    assunto=assunto, chave_input=3)
                dc.gerar_dados_total_por_canal_turno(
                    canal=canal_tres, assunto=assunto, flag=2, coluna_analise=coluna_analise)

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
                canal = dc.listar_canais_assunto(
                    assunto=assunto, chave_input=4)
                dc.gerar_dados_canal_dia(
                    canal=canal, assunto=assunto, flag=2, coluna_analise=coluna_analise)
            with tab2:
                coluna_analise = 'total_inscritos'
                canal = dc.listar_canais_assunto(
                    assunto=assunto, chave_input=5)
                dc.gerar_dados_canal_dia(
                    canal=canal, assunto=assunto, flag=2, coluna_analise=coluna_analise)
            with tab3:
                coluna_analise = 'total_videos_publicados'
                canal = dc.listar_canais_assunto(
                    assunto=assunto, chave_input=6)
                dc.gerar_dados_canal_dia(
                    canal=canal, assunto=assunto, flag=2, coluna_analise=coluna_analise)

    with st.container():
        st.write('Análise vídeo')

        tab1, tab2, tab3 = st.tabs(
            ['Total vísualizações', 'Total Likes', 'Total Comentários']
        )

        with st.container():
            with tab1:

                st.write(
                    'Total Visualizações Turno '
                )

                coluna_analise = 'total_visualizacoes'
                nome_video = dc.listar_video_assunto(
                    assunto=assunto, chave_input=7)
                dc.gerar_dados_videos(
                    video=nome_video, assunto=assunto, flag=2, coluna_analise=coluna_analise)
            with tab2:
                st.write(
                    ' total Likes Turno'
                )
                coluna_analise = 'total_visualizacoes'
                nome_video = dc.listar_video_assunto(
                    assunto=assunto, chave_input=8)
                dc.gerar_dados_videos(
                    video=nome_video, assunto=assunto, flag=2, coluna_analise=coluna_analise)

            with tab3:
                st.write(
                    'total  Comentário Turno'
                )
                coluna_analise = 'total_comentarios'
                nome_video = dc.listar_video_assunto(
                    assunto=assunto, chave_input=9)
                dc.gerar_dados_videos(
                    video=nome_video, assunto=assunto, flag=2, coluna_analise=coluna_analise)

    with st.container():
        st.write('Análise Taxa engajamento')

        tab1, tab2, tab3 = st.tabs(
            [
                'Média engajamento canal vísualização',
                'Média engajamento total inscritos ',
                'Média engajamento vídeo'
            ]
        )

        with tab1:
            st.write('Média engajamento  do  canal por visualização')
            canal = dc.listar_canais_assunto_multiplos(
                assunto=assunto, chave_input=11)
            dc.gerar_dados_engajamento_canal_visualizacao(
                assunto=assunto, flag=2, canal=canal)

        with tab2:
            st.write('Média taxa engajamento do canal por total de inscritos')
            canal = dc.listar_canais_assunto(assunto=assunto, chave_input=12)

        with tab3:
            st.write('Taxa engajamento vídeo')

            canal = dc.listar_canais_assunto(assunto=assunto, chave_input=13)


main()
