from typing import Dict, List
import requests

from src.dados.infra_pickle import InfraPicke


class DadosYoutube():

    @classmethod
    def verificar_idioma_canal(cls, req: Dict) -> bool:
        """Método para verificar se o canal é brasileiro

        Args:
            id_canal (str): id do canal

        Returns:
            bool: verdadeiro ou falso
        """
        try:

            flag = req['items'][0]['snippet']['country']
            if flag == 'BR':
                return True
            return False
        except:
            return False

    @classmethod
    def obter_lista_videos(cls, req: Dict) -> List[str]:
        """Método para obter os vídeos dos canais brasileiros

        Args:
            req (Dict): requisição da api do youtube

        Returns:
            List[str]: Lista de vídeos Brasileiros
        """
        lista_videos = []
        for item in req['items']:
            if cls.verificar_idioma_canal(item['snippet']['channelId']):
                lista_videos.append(item['id']['videoId'])
        return list(set(lista_videos))
