from src.dados.arquivo import Arquivo
from typing import Union, Any
from config.unidades import ListaCanaisVideo, ListaVideos
import pickle
import os


class ArquivoPicke(Arquivo[Union[ListaVideos, ListaCanaisVideo]]):

    def __init__(self, pasta_datalake, camada_datalake, caminho_path_data, assunto, nome_arquivo, metrica=None):
        """_summary_

        Args:
            pasta_datalake (str): o nome raíz do datalake
            camada_datalake (str): a camada do datalake, bronze, prata, ouro
            assunto (str): é o assunto de pesquisa
            nome_arquivo (str): o nome do arquivo
            caminho_path_data (str): È o que vai armazenar a data de extracao Ex: extracao_dia_2024_10_27_15_00_00_tarde
            metrica (Optional[str], optional): o nome da métrica. Defaults to None.
        """
        super().__init__(pasta_datalake, camada_datalake,
                         caminho_path_data, assunto, nome_arquivo, metrica)

    def salvar_dados(self, dados: Union[ListaVideos, ListaCanaisVideo]):
        """Método para salvar dados

        Args:
            dados (Union[ListaVideos, ListaCanaisVideo]): Uma lista de vídeos ou uma lista de par (id_canal, video)
        """
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
        """Método para carregar dados

        Returns:
            Union[ListaVsideos, ListaCanaisVideo]: _description_
        """
        if os.path.exists(self._diretorio_completo):
            caminho_arquivo = os.path.join(
                self._diretorio_completo, self._nome_arquivo)
            with open(caminho_arquivo, 'rb') as arquivo_pickle:
                lista = pickle.load(arquivo_pickle)
        return lista
