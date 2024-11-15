from src.config.conexao import ConexaoBancoHive
from src.config.config_banco import Base

import pandas as pd


class Medida:

    def __init__(self):
        self.__db = ConexaoBancoHive()
        self.__conexao = self.__db.obter_conexao()
        self.__Sessao = self.__db.obter_sessao()
        Base.metadata.create__all(self.__conexao)

    def obter_depara_video(self, assunto: str):
        sql = f"""
            SELECT 
                id_video,
                titulo_video 
            from depara_video  
            WHERE assunto = ?
        """

        parametros = (assunto)
        try:
            tipos = {
                'id_video': 'string',
                'titulo_video': 'string'
            }
            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, dtype=tipos, params=parametros)
        finally:
            self.__Sessao.close()
        return dataframe

    def obter_depara_canal(self, assunto: str):
        sql = """
            SELECT 
                id_canal,
                nm_canal
                
                
            from depara_canais  
            where assunto = :assunto

        """
        parametros = {
            'assunto': assunto
        }
        try:
            tipos = {
                'id_canal': 'string',
                'nm_canal': 'string'
            }
            dataframe = pd.read_sql_query(
                sql=sql,
                con=self.__conexao,
                dtype=tipos,
                params=parametros
            )
        finally:
            self.__Sessao.close()
        return dataframe

    def obter_total_variacao_dados_canal_turno(self, assunto: str, id_canal: str, coluna_analise: str) -> pd.DataFrame:
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
                {coluna_analise},
                case when  {coluna_analise} - (LAG( {coluna_analise}, 1) OVER(PARTITION BY id_canal ORDER BY data_extracao)) IS NULL 
                    then 0 
                else   {coluna_analise} - (LAG( {coluna_analise}, 1) OVER(PARTITION BY id_canal ORDER BY data_extracao)) end as {coluna_analise}_turno
            FROM 
                estatisticas_canais 
            WHERE 
                assunto = :assunto
                AND id_canal = :id_canal
                
            ORDER BY 
                data_extracao ASC;
        """

        parametros = {
            'assunto': assunto,
            'id_canal': id_canal
        }

        try:
            tipos = {
                'turno_extracao': 'string',
                'dia_da_semana': 'string',
                'total_videos_publicados': 'int64',
                'total_videos_publicados_turno': 'int64'
            }

            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, dtype=tipos, params=parametros)

        finally:
            self.__Sessao.close()
        return dataframe

    def obter_dado_canal_dia(self, assunto: str, id_canal: str, coluna_analise: str):
        sql = f"""
            SELECT 
            data_extracao,
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
            {coluna_analise},
            COALESCE (LAG({coluna_analise}, 1) OVER(PARTITION BY id_canal ORDER BY data_extracao), 0) AS {coluna_analise}_anterior,
            COALESCE ({coluna_analise} - LAG({coluna_analise}, 1) OVER(PARTITION BY id_canal ORDER BY data_extracao), 0) as {coluna_analise}_dia
        FROM 
            estatisticas_canais ec 
        WHERE 
            assunto = :assunto
            AND id_canal = :id_canal
            AND turno_extracao = 'Noite'
        ORDER BY 
            data_extracao ASC
   
        """

        parametros = {
            'assunto': assunto,
            'id_canal': id_canal
        }

        try:
            tipos = {
                'data_extracao': 'string',
                'dia_da_semana': 'string',
                {coluna_analise}: 'int64',
                f'{coluna_analise}_dia': 'int64'
            }

            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, dtype=tipos, params=parametros)

        finally:
            self.__Sessao.close()
        return dataframe

    def obter_total_dados_video_turno(self, id_video: str, assunto: str):
        sql = f"""
            SELECT 
            ev.turno_extracao AS turno_extracao,
            CASE 
                WHEN COALESCE(
                    ev.total_visualizacoes - 
                    LAG(ev.total_visualizacoes, 1) OVER (PARTITION BY id_canal ORDER BY data_extracao), 
                    0
                ) = 0
                THEN ev.total_visualizacoes
                ELSE COALESCE(
                    ev.total_visualizacoes - 
                    LAG(ev.total_visualizacoes, 1) OVER (PARTITION BY id_canal ORDER BY data_extracao), 
                    0
                )
            END AS total_visualizacoes_turno
        FROM 
            estatisticas_videos ev
        WHERE 
            ev.assunto = :assunto
            AND ev.id_video = :id_video

    """
        parametros = {
            'assunto': assunto,
            'id_video': id_video
        }

        try:
            tipos = {
                'turno_extracao': 'string',
                'total_visualizacoes_turno': 'int64',

            }
            dataframe = pd.read_sql_query(
                sql=sql,
                con=self.__conexao,
                dtype=tipos,
                params=parametros

            )

        finally:
            self.__Sessao.close()
        return dataframe
