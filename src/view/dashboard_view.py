import streamlit as st


class DashboardView:
    def mostrar_usuarios(self, dataframe):
        st.dataframe(dataframe)
