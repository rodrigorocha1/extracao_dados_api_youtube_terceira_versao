from src.config.conexao import ConexaoBancoHive
from src.config.config_banco import Base
from typing import Any, List, Optional, Sequence, Tuple, Union
import pandas as pd


class Medida:

    def __init__(self):
        self.__db = ConexaoBancoHive()
        self.__conexao = self.__db.obter_conexao()
        self.__Sessao = self.__db.obter_sessao()
        Base.metadata.create_all(self.__conexao)

    def obter_depara_video(self, assunto: str,
                           flag: int, titulo_video: str,
                           id_canal: Union[Optional[str], List[str]] = None) -> pd.DataFrame:
        """Método para obter os dados do vídeo      

        Args:
            assunto (str): assunto a ser pesquisado
            flag (int): flag direcionamento 1 = assunto, 2 = assunto e título  e 3 assunto e id canal
            titulo_video (str): nome do vídeo 
            id_canal (str, optional): _description_. id do canal to None.

        Returns:
            pd.DataFrame: dataframe do pandas com id_video e título vídeo, dataframe com id_video ou um dataframe com título vídeo
        """
        parametros: Union[List[Any], Tuple[Any, ...]]
        if flag == 1:
            sql = f"""
                SELECT
                    id_video,
                    titulo_video
                FROM
                    depara_video
                WHERE
                    assunto = %s
            """
            parametros = (assunto)
            tipos = {
                'id_video': 'string',
                'titulo_video': 'string'
            }
        elif flag == 2:
            sql = f"""
               SELECT
                    id_video
                from depara_video
                WHERE  assunto = %s
                and titulo_video =  %s
            """
            tipos = {
                'id_video': 'string'
            }
            parametros = (assunto, titulo_video)
        else:
            canal_placeholder = ', '.join(['%s'] * len(id_canal))
            parametros = (assunto, *id_canal)
            sql = f"""
                SELECT
                    titulo_video
                FROM
                    depara_video
                WHERE
                    assunto = %s
                    AND id_canal IN ({canal_placeholder})
            """
            tipos = {
                'titulo_video': 'string'
            }
            print(sql % parametros)

        try:
            dataframe = pd.read_sql_query(
                sql=sql, con=self.__conexao, dtype=tipos, params=parametros)
        finally:
            self.__Sessao.close()

        return dataframe

    def obter_depara_canal(self, assunto: str, flag: int, nm_canal: Optional[str]) -> pd.DataFrame:
        """Método para obter os dados canal

        Args:
            assunto (str): assunto de pesquisa
            flag (int): flag de direcionamento 1 - assunto/2 - assunto e nome canal/ 
            nm_canal (Optional[str]): nome do canal 

        Returns:
            pd.DataFrame: dataframe com id canal e nome canal ou um dataframe com o id canal
        """
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
            parametros: Tuple[str, ...] = (assunto,)
            tipos = {
                'id_canal': 'string',
                'nm_canal': 'string'
            }
        else:
            if isinstance(nm_canal, str) or nm_canal is None:
                canal_placeholder = '%s'
                parametros = (
                    assunto, nm_canal) if nm_canal is not None else (assunto,)
            else:

                canal_placeholder = ', '.join(['%s'] * len(nm_canal))
                parametros = (assunto, *nm_canal)

            sql = f"""
                SELECT
                    id_canal
                from
                    depara_canais
                WHERE
                    assunto = %s
                    and nm_canal in  ({canal_placeholder})
            """

            tipos = {
                'id_canal': 'string'
            }
        print(sql % parametros)
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
        """ Método para obter as análises do canal por turno

        Args:
            assunto (str): assunto de pesquisa
            id_canal (str): id_canal
            coluna_analise (str): coluna de análise [total_visualizacoes, total_inscritos, total_videos_publicados]

        Returns:
            pd.DataFrame: dataframe com as análises
        """
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

    def obter_dado_canal_dia(self, assunto: str, id_canal: str, coluna_analise: str) -> pd.DataFrame:
        """Método para obter os dados do canal por dia

        Args:
            assunto (str): assunto de pesquisa  
            id_canal (str): id do canal
            coluna_analise (str): coluna de análise [total_visualizacoes, total_inscritos, total_videos_publicados]

        Returns:
            pd.DataFrame: dataframe com as análise
        """
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

    def obter_total_dados_video_turno(self, id_video: str, assunto: str, coluna_analise: str) -> pd.DataFrame:
        """Método para obter as análises do vídeo por turno

        Args:
            id_video (str): id do vídeo
            assunto (str): assunto de pesquisa
            coluna_analise (str): [total_visualizacoes, total_likes, total_comentarios]

        Returns:
            pd.DataFrame: dataframe com as análises
        """
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

    def obter_media_taxa_engajamento_canal_total_inscritos(self, assunto: str, ids_canal: List[str]) -> pd.DataFrame:
        """Método para obter taxa engajamento total inscritos

        Args:
            assunto (str): assunto de pesquisa
            ids_canal (List[str]): lista de canais

        Returns:
            pd.DataFrame: dataframe com os dados
        """
        ids_canal_placeholder = ', '.join(
            ['%s'] * len(ids_canal)) if isinstance(ids_canal, list) else '%s'

        parametros = (assunto, *ids_canal, assunto, *ids_canal)

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
                    AND ec.id_canal IN ({ids_canal_placeholder})
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
                        (ev.total_likes + ev.total_comentarios) / NULLIF(canal_info.total_inscritos, 0) * 100, 
                        0
                    ) AS taxa_engajamento_inscritos,
                    CASE 
                        WHEN date_format(ev.data_extracao, 'EEEE') = 'Monday' THEN 'Segunda-feira'
                        WHEN date_format(ev.data_extracao, 'EEEE') = 'Tuesday' THEN 'Terça-feira'
                        WHEN date_format(ev.data_extracao, 'EEEE') = 'Wednesday' THEN 'Quarta-feira'
                        WHEN date_format(ev.data_extracao, 'EEEE') = 'Thursday' THEN 'Quinta-feira'
                        WHEN date_format(ev.data_extracao, 'EEEE') = 'Friday' THEN 'Sexta-feira'
                        WHEN date_format(ev.data_extracao, 'EEEE') = 'Saturday' THEN 'Sábado'
                        WHEN date_format(ev.data_extracao, 'EEEE') = 'Sunday' THEN 'Domingo'
                    END AS dia_da_semana,
                    dayofweek(ev.data_extracao) AS dia_semana
                FROM 
                    estatisticas_videos ev
                JOIN 
                    canal_info ON ev.id_canal = canal_info.id_canal
                WHERE 
                    ev.assunto = %s 
                    AND ev.id_canal IN ({ids_canal_placeholder})
            )
            SELECT 
                vi.id_canal, 
                ci.nm_canal, 
                vi.dia_da_semana, 
                vi.dia_semana, 
                ROUND(AVG(vi.taxa_engajamento_inscritos), 2) AS media_taxa_engajamento
            FROM 
                video_info vi
            JOIN 
                canal_info ci ON vi.id_canal = ci.id_canal
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

    def obter_media_engajamento_canal_visualizacoes(self, ids_canal: List, assunto: str) -> pd.DataFrame:
        """Método para obter média engajamento do canal por vísualizações

        Args:
            ids_canal (List): lista com os id do canal
            assunto (str): assunto do canal

        Returns:
            pd.DataFrame: dataframe pandas
        """
        ids_canal_placeholder = ', '.join(
            ['%s'] * len(ids_canal)) if isinstance(ids_canal, list) else '%s'

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
                AND dc.id_canal  in  ({ids_canal_placeholder})
            ) dcc on dcc.id_canal = ev.id_canal
            where ev.assunto = %s
            AND ev.id_canal  in ({ids_canal_placeholder})
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

        parametros = (assunto, *ids_canal, assunto, *ids_canal)
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

    def obter_taxa_engajamento_video(self, assunto: str, id_video: List[str]) -> pd.DataFrame:
        """Método para comparar a taxa de engajamento do vídeo

        Args:
            assunto (str): assunto vídeo
            id_video (List[str]): lista vídeo

        Returns:
            pd.DataFrame: dataframe pandas
        """
        video_placeholder = ', '.join(['%s'] * len(id_video))
        parametros = (assunto, *id_video)
        sql = f"""
            WITH engajamento_dia AS (
                SELECT 
                    ev.titulo_video,
                    ev.data_extracao,
                    ev.turno_extracao,
                    ev.total_likes,
                    ev.total_comentarios,
                    ev.total_visualizacoes,
                    COALESCE(
                    (ev.total_likes + ev.total_comentarios) / NULLIF(ev.total_visualizacoes, 0) * 100, 
                    0
                    ) AS taxa_engajamento_total,
                    -- Calcula as diferenças diárias utilizando LAG
                    LAG(ev.total_likes) OVER (
                    PARTITION BY ev.id_video 
                    ORDER BY ev.data_extracao
                    ) AS likes_anteriores,
                    LAG(ev.total_comentarios) OVER (
                    PARTITION BY ev.id_video 
                    ORDER BY ev.data_extracao
                    ) AS comentarios_anteriores,
                    LAG(ev.total_visualizacoes) OVER (
                    PARTITION BY ev.id_video 
                    ORDER BY ev.data_extracao
                    ) AS visualizacoes_anteriores
                FROM 
                    estatisticas_videos ev
                WHERE 
                    ev.assunto = %s
                    AND ev.id_video IN ({video_placeholder})
                    and ev.turno_extracao = 'Noite'
                )

                SELECT 
                titulo_video,
                CASE date_format(data_extracao, 'EEEE')
                            WHEN 'Monday' THEN 'Segunda-feira'
                            WHEN 'Tuesday' THEN 'Terça-feira'
                            WHEN 'Wednesday' THEN 'Quarta-feira'
                            WHEN 'Thursday' THEN 'Quinta-feira'
                            WHEN 'Friday' THEN 'Sexta-feira'
                            WHEN 'Saturday' THEN 'Sábado'
                            WHEN 'Sunday' THEN 'Domingo'
                        END as dia,

                COALESCE(
                    ((total_likes - likes_anteriores) + (total_comentarios - comentarios_anteriores)) / 
                    NULLIF((total_visualizacoes - visualizacoes_anteriores), 0) * 100,
                    0
                ) AS taxa_engajamento_dia
                FROM 
                engajamento_dia
                WHERE COALESCE(
                    ((total_likes - likes_anteriores) + (total_comentarios - comentarios_anteriores)) / 
                    NULLIF((total_visualizacoes - visualizacoes_anteriores), 0) * 100,
                    0
                )  > 0
                ORDER BY 
                data_extracao

        """

        try:
            tipos = {
                "titulo_video": "string",
                "dia": "string",
                "taxa_engajamento_dia": "string"

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
