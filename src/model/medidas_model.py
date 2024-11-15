from src.config.conexao import ConexaoBancoHive
from src.config.config_banco import Base
from typing import List
import pandas as pd


class Medida:

    def __init__(self):
        self.__db = ConexaoBancoHive()
        self.__conexao = self.__db.obter_conexao()
        self.__Sessao = self.__db.obter_sessao()
        Base.metadata.create_all(self.__conexao)

    def obter_depara_video(self, assunto: str):
        sql = f"""
            
                SELECT
                    id_video,
                    titulo_video
                from
                    depara_video
                WHERE
                    assunto = %s
        """

        parametros = (assunto, )
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
            from
                depara_canais
            WHERE
                assunto = % s
        """
        parametros = (assunto,)
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
                    CASE
                        date_format(data_extracao, 'EEEE')
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
                case
                    when {coluna_analise} - (
                        LAG({coluna_analise}, 1) OVER(
                            PARTITION BY id_canal
                            ORDER BY
                                data_extracao
                        )
                    ) IS NULL then 0
                    else {coluna_analise} - (
                        LAG({coluna_analise}, 1) OVER(
                            PARTITION BY id_canal
                            ORDER BY
                                data_extracao
                        )
                    )
                end as {coluna_analise}_turno
            FROM
                estatisticas_canais
            WHERE
                assunto = %s
                AND id_canal = %s
            ORDER BY
                data_extracao ASC
        """

        parametros = (assunto, id_canal)

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
                    CASE
                        date_format(data_extracao, 'EEEE')
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
                COALESCE (
                    LAG({coluna_analise}, 1) OVER(
                        PARTITION BY id_canal
                        ORDER BY
                            data_extracao
                    ),
                    0
                ) AS {coluna_analise} _anterior,
                COALESCE (
                    {coluna_analise} - LAG({coluna_analise}, 1) OVER(
                        PARTITION BY id_canal
                        ORDER BY
                            data_extracao
                    ),
                    0
                ) as {coluna_analise}_dia
            FROM
                estatisticas_canais ec
            WHERE
                assunto = % s
                AND id_canal = % s
                AND turno_extracao = 'Noite'
            ORDER BY
                data_extracao ASC
   
        """

        parametros = (assunto, id_canal)
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
            ev.assunto =  %s
            AND ev.id_video =  %s

    """
        parametros = (assunto, id_video)

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

    def obter_media_taxa_engajamento_dia(self, assunto: str, ids_video: List[str]):
        placeholders = ', '.join(
            [f":id_video_{i}" for i in range(len(ids_video))])
        sql = f"""
            SELECT   
                ev.id_video as id_video,
                dvv.titulo_video as titulo_video, 
                regexp_replace(
                    date_format(ev.data_extracao, 'EEEE'),
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
                ) AS dia_da_semana ,

                COALESCE(ROUND(AVG(((ev.total_likes + ev.total_comentarios ) / ev.total_visualizacoes) * 100), 2), 0) as media_taxa_engajamento
            from estatisticas_videos ev 
            INNER JOIN (
                SELECT *
                FROM depara_video dv 
                WHERE dv.assunto = :assunto
                AND dv.id_video  IN ({placeholders})
            ) dvv on dvv.id_video = ev.id_video
            where ev.assunto = :assunto
            AND ev.id_video  IN ({placeholders})
            GROUP  BY ev.id_video , dvv.titulo_video , regexp_replace(
                    date_format(ev.data_extracao, 'EEEE'),
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
                ) , dayofweek(ev.data_extracao) 
            HAVING   COALESCE(ROUND(AVG(((ev.total_likes + ev.total_comentarios ) / ev.total_visualizacoes) * 100), 2), 0) > 0
            ORDER BY   dayofweek(ev.data_extracao)
        """

        parametros = {'assunto': assunto}
        for i, id_video in enumerate(ids_video):
            parametros[f'id_video_{i}'] = id_video

        try:
            tipos = {
                'id_video': 'string',
                'titulo_video': 'string',
                'dia_da_semana': 'string',
                'media_taxa_engajamento': 'float64'

            }
            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, params=parametros, dtype=tipos)

        finally:
            self.__Sessao.close()
        return dataframe

    def obter_media_engajamento_canal(self, ids_canal: List[str], assunto: str):

        placeholders = ', '.join(
            [f":id_video_{i}" for i in range(len(ids_canal))])
        sql = f"""
        SELECT   
            ev.id_canal as id_canal ,
            dcc.nm_canal as nm_canal, 
            regexp_replace(
                date_format(ev.data_extracao, 'EEEE'),
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

            COALESCE(ROUND(AVG(((ev.total_likes + ev.total_comentarios ) / ev.total_visualizacoes) * 100), 2), 0) as media_taxa_engajamento
        from estatisticas_videos ev 
        INNER JOIN (
            SELECT *
            FROM depara_canais dc  
            WHERE dc.assunto = '{assunto}' 
            AND dc.id_canal  in ({placeholders})
        ) dcc on dcc.id_canal = ev.id_canal 
        where ev.assunto = '{assunto}' 
        AND ev.id_canal  in ({placeholders})
        GROUP  BY ev.id_canal ,dcc.nm_canal , regexp_replace(
                date_format(ev.data_extracao, 'EEEE'),
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
            ),dayofweek(ev.data_extracao)
        HAVING   COALESCE(ROUND(AVG(((ev.total_likes + ev.total_comentarios ) / ev.total_visualizacoes) * 100), 2), 0) > 0
        ORDER BY   	dayofweek(ev.data_extracao) 
    """

        parametros = {'assunto': assunto}
        for i, id_canal in enumerate(ids_canal):
            parametros[f'id_video_{i}'] = id_canal

        try:

            tipos = {
                'id_video': 'string',
                'titulo_video': 'string',
                'dia_da_semana': 'string',
                'media_taxa_engajamento': 'float64'

            }

            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, params=parametros, dtype=tipos)

        finally:

            self.__Sessao.close()

        return dataframe
