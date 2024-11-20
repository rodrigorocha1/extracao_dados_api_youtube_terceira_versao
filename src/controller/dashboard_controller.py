from src.model.medidas_model import Medida
from typing import Tuple
import pandas as pd


class DashboardController:

    def __init__(self):
        self.__model = Medida()

    def listar_canais_assunto(self,  assunto: str) -> Tuple[str]:
        """Método para listar canais

        Args:
            assunto (str): assunto

        Returns:
            Tuple[str]: tupla com os nomes dos canais
        """
        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        dados_canal = tuple(dataframe['nm_canal'].to_list())
        return dados_canal

    def gerar_resultado_total_canais_turno(self, assunto: str, coluna_analise: str, nome_canal: str) -> pd.DataFrame:
        id_canal = self.__model.obter_depara_canal(
            assunto=assunto, flag=2, nm_canal=nome_canal)
        id_canal = id_canal.to_string().split(' ')[-1]
        dataframe = self.__model.obter_dados_canal_turno(
            assunto=assunto, id_canal=id_canal, coluna_analise=coluna_analise)
        return dataframe
