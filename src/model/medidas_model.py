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

    def obter_depara_video(self, assunto: str, flag: int = None, titulo_video: str = None):

        if flag == 1:
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
            tipos = {
                'id_video': 'string',
                'titulo_video': 'string'
            }
        else:
            sql = f"""
                
                    SELECT
                        id_video
                        
                    from
                        depara_video
                    WHERE
                        assunto = %s
                        and titulo_video = %s
            """
            tipos = {
                'id_video': 'string'
            }
            parametros = (assunto, titulo_video)

        try:

            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, dtype=tipos, params=parametros)
        finally:
            self.__Sessao.close()
        return dataframe

    def obter_depara_canal(self, assunto: str, flag: int = None, nm_canal: str = None):
        if flag == 1:
            sql = """
                SELECT
                    id_canal,
                    nm_canal
                from
                    depara_canais
                WHERE
                    assunto = %s
            """
            parametros = (assunto,)
            tipos = {
                'id_canal': 'string',
                'nm_canal': 'string'
            }
        else:
            sql = """
                SELECT
                    id_canal
                from
                    depara_canais
                WHERE
                    assunto = %s
                    and nm_canal in  (%s)
            """
            parametros = (assunto, nm_canal)
            tipos = {
                'id_canal': 'string'
            }

        try:

            dataframe = pd.read_sql_query(
                sql=sql,
                con=self.__conexao,
                dtype=tipos,
                params=parametros
            )
        finally:
            self.__Sessao.close()
        return dataframe

    def obter_dados_canal_turno(self, assunto: str, id_canal: str, coluna_analise: str) -> pd.DataFrame:
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
                coluna_analise: 'int64',
                f'{coluna_analise}': 'int64'
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
                ) AS {coluna_analise}_anterior,
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
                assunto = %s
                AND id_canal = %s
                AND turno_extracao = 'Noite'
            ORDER BY
                data_extracao ASC
   
        """

        parametros = (assunto, id_canal)
        try:
            tipos = {
                'data_extracao': 'string',
                'dia_da_semana': 'string',
                coluna_analise: 'int64',
                f'{coluna_analise}_dia': 'int64'
            }

            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, dtype=tipos, params=parametros)

        finally:
            self.__Sessao.close()
        return dataframe

    def obter_total_dados_video_turno(self, id_video: str, assunto: str, coluna_analise: str):
        sql = f"""
            SELECT 
                ev.turno_extracao AS turno_extracao,
                regexp_replace(
                    date_format(ev.data_extracao, 'EEEE'),
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
                ) as dia_semana,
            CASE 
                WHEN COALESCE(
                    ev.{coluna_analise} - 
                    LAG(ev.{coluna_analise}, 1) OVER (PARTITION BY id_canal ORDER BY data_extracao), 
                    0
                ) = 0
                THEN ev.{coluna_analise}
                ELSE COALESCE(
                    ev.{coluna_analise} - 
                    LAG(ev.{coluna_analise}, 1) OVER (PARTITION BY id_canal ORDER BY data_extracao), 
                    0
                )
            END AS {coluna_analise}_turno
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
                f'{coluna_analise}_turno': 'int64',

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

    def obter_media_taxa_engajamento_canal_total_inscritos(self, assunto: str, ids_canal: List[str]):
        id_canal = ', '.join(id_canal for id_canal in ids_canal)
        parametros = (assunto, id_canal, assunto, id_canal)

        sql = f"""
            WITH canal_info AS (
                SELECT
                    ec.id_canal,
                    ec.nm_canal,
                    ec.total_inscritos
                FROM
                    estatisticas_canais ec
                WHERE
                    ec.assunto = %s
                    AND ec.id_canal IN (
                        %s
                    )
            ),
            video_info AS (
                SELECT
                    ev.id_canal,
                    ev.data_extracao,
                    COALESCE(
                        (ev.total_likes + ev.total_comentarios) / NULLIF(ev.total_visualizacoes, 0) * 100,
                        0
                    ) AS taxa_engajamento,
                    COALESCE(
                        (ev.total_likes + ev.total_comentarios) / NULLIF(dcc.total_inscritos, 0) * 100,
                        0
                    ) AS taxa_engajamento_inscritos,
                    CASE
                        date_format(ev.data_extracao, 'EEEE')
                        WHEN 'Monday' THEN 'Segunda-feira'
                        WHEN 'Tuesday' THEN 'Terça-feira'
                        WHEN 'Wednesday' THEN 'Quarta-feira'
                        WHEN 'Thursday' THEN 'Quinta-feira'
                        WHEN 'Friday' THEN 'Sexta-feira'
                        WHEN 'Saturday' THEN 'Sábado'
                        WHEN 'Sunday' THEN 'Domingo'
                    END AS dia_da_semana,
                    dayofweek(ev.data_extracao) AS dia_semana
                FROM
                    estatisticas_videos ev
                    JOIN canal_info dcc ON ev.id_canal = dcc.id_canal
                WHERE
                    ev.assunto = %s
                    AND ev.id_canal IN (
                        %s
                    )
            )
            SELECT
                vi.id_canal,
                ci.nm_canal,
                vi.dia_da_semana,
                vi.dia_semana,
                ROUND(AVG(vi.taxa_engajamento_inscritos), 2) AS media_taxa_engajamento
            FROM
                video_info vi
                JOIN canal_info ci ON vi.id_canal = ci.id_canal
            GROUP BY
                vi.id_canal,
                ci.nm_canal,
                vi.dia_da_semana,
                vi.dia_semana
            HAVING
                AVG(vi.taxa_engajamento) > 0
            ORDER BY
                vi.dia_semana
	
	
	
        """

        try:

            tipos = {
                'id_canal': 'string',
                'nm_canal': 'string',
                'dia_da_semana': 'string',
                'media_taxa_engajamento': 'float64'

            }

            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, params=parametros, dtype=tipos)

        finally:

            self.__Sessao.close()

        return dataframe

    def obter_media_engajamento_canal(self, ids_canal: List[str], assunto: str):

        id_canal = ', '.join(id_canal for id_canal in ids_canal)

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
                WHERE dc.assunto = %s
                AND dc.id_canal  in  (%s)
            ) dcc on dcc.id_canal = ev.id_canal 
            where ev.assunto = %s
            AND ev.id_canal  in (%s)
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

        parametros = (assunto, id_canal, assunto, id_canal)
        try:

            tipos = {
                'id_canal': 'string',
                'nm_canal': 'string',
                'dia_da_semana': 'string',
                'media_taxa_engajamento': 'float64'

            }

            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, params=parametros, dtype=tipos)

        finally:

            self.__Sessao.close()

        return dataframe
