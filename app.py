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

        canal = dc.listar_canais_assunto(assunto, 1)
        st.write(canal)
    canal2 = dc.listar_canais_assunto(assunto, 2)

    st.write(canal2)

    with st.container():
        st.write('Análise vídeo')

    with


main()
