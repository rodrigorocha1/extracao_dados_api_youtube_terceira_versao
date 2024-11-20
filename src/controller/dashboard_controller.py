from src.model.medidas_model import Medida
from typing import Tuple


class DashboardController:

    def __init__(self):
        self.__model = Medida()

    def listar_canais_assunto(self,  assunto: str) -> Tuple[str]:
        dataframe = self.__model.obter_depara_canal(
            assunto=assunto, flag=1, nm_canal=None)
        dados_canal = tuple(dataframe['nm_canal'].to_list())
        return dados_canal
