from src.config.conexao import ConexaoBancoHive
from src.config.config_banco import Base

import pandas as pd


class Medida:

    def __init__(self):
        self.__db = ConexaoBancoHive()
        self.__conexao = self.__db.obter_conexao()
        self.__Sessao = self.__db.obter_sessao()
        Base.metadata.create__all(self.__conexao)

    def obter_total_variacao_video_canal(self, assunto: str, id_canal: str,) -> pd.DataFrame:
        sql = f"""
            SELECT 
            turno_extracao,
            regexp_replace(
                    date_format(data_extracao, 'EEEE'),
                    'Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday',
                    CASE date_format(data_extracao, 'EEEE')
                        WHEN 'Monday' THEN 'Segunda-feira'
                        WHEN 'Tuesday' THEN 'Terça-feira'
                        WHEN 'Wednesday' THEN 'Quarta-feira'
                        WHEN 'Thursday' THEN 'Quinta-feira'
                        WHEN 'Friday' THEN 'Sexta-feira'
                        WHEN 'Saturday' THEN 'Sábado'
                        WHEN 'Sunday' THEN 'Domingo'
                    END
                ) AS dia_da_semana,
                total_videos_publicados ,
                case when total_videos_publicados - (LAG(total_videos_publicados, 1) OVER(PARTITION BY id_canal ORDER BY data_extracao)) IS NULL 
                    then 0 
                else  total_videos_publicados - (LAG(total_videos_publicados, 1) OVER(PARTITION BY id_canal ORDER BY data_extracao)) end as total_videos_publicados_turno
            FROM 
                estatisticas_canais 
            WHERE 
                assunto = '{assunto}'
                AND id_canal = '{id_canal}'
                
            ORDER BY 
                data_extracao ASC;
        """
        try:

            resultado = self.__conexao.execute(sql)
            dataframe = pd.DataFrame(
                resultado.fetchall(), columns=resultado.keys)
        finally:
            self.__Sessao.close()
        return dataframe
