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
                ['Total vísualizações', 'Total Inscritos', 'Total Vídeo'])

            with tab1:
                st.write('Total vísualizações')

            with tab2:
                st.write('Total Inscritos')

            with tab3:
                st.write('Total Vídeo')

        with col2:
            st.write(
                '# 2 - Total vísualizações/inscritos/videos_publicado por dia canal'
            )

    with st.container():
        st.write('Análise vídeo')

        col1, col2, col3 = st.columns(3)
        with col1:
            st.write(
                '# 3-  TOTAL Visualizações, total comentários e total_likes vídeo Turno '
            )
        with col2:
            st.write(
                '  # 4 - Total Visualizações, total comentários e total_likes  vídeo dia'
            )

        with col3:
            st.write(
                ' # 5 - Média da taxa de engajamento do vídeo por dia'
            )

    with st.container():
        st.write('Análise Taxa engajamento')

        col1, col2 = st.columns(2)

        with col1:
            st.write(' -Média engajamento  do  canal por visualização')

        with col2:
            st.write('8 Fequência de vídeos publicados por assunto')


main()
