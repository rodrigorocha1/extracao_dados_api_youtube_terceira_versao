from src.dados.arquivo import Arquivo
from typing import Union, Any
from config.unidades import ListaCanaisVideo, ListaVideos
import pickle
import os


class ArquivoPicke(Arquivo[Union[ListaVideos, ListaCanaisVideo]]):

    def __init__(self, pasta_datalake, camada_datalake, assunto, nome_arquivo, metrica=None):
        """_summary_

        Args:
            pasta_datalake (str): o nome raíz do datalake
            camada_datalake (str): a camada do datalake, bronze, prata, ouro
            assunto (str): é o assunto de pesquisa
            nome_arquivo (str): métrica de pesquisa
            metrica (str, optional): o nome do arquivo. Defaults to None.
        """
        super().__init__(pasta_datalake, camada_datalake, assunto, nome_arquivo, metrica)

    def salvar_dados(self, dados: Union[ListaVideos, ListaCanaisVideo]):
        if not os.path.exists(self._diretorio_completo):
            os.makedirs(self._diretorio_completo, exist_ok=True)

        else:
            try:
                lista = self.carregar_dados()

                var = dados + lista

                var = list(set(var))
            except:
                var = dados

        with open(os.path.join(self._diretorio_completo, self._nome_arquivo), 'wb') as arquivo_pickle:
            if arquivo_pickle is not None:
                pickle.dump(var, arquivo_pickle)

    def carregar_dados(self) -> Union[ListaVideos, ListaCanaisVideo]:
        if os.path.exists(self._diretorio_completo):
            with open(self._diretorio_completo, 'rb') as arquivo_pickle:
                lista = pickle.load(arquivo_pickle)
        return lista
