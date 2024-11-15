from src.config.conexao import ConexaoBancoHive


cbh = ConexaoBancoHive()
conexao_hive = cbh.obter_conexao()


with conexao_hive.connect() as conn:

    sql = """
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
            nm_canal,
            turno_extracao,
            total_videos_publicados ,
            LAG(total_videos_publicados, 1) OVER(PARTITION BY id_canal ORDER BY data_extracao) AS total_videos_publicados_anterior
        FROM 
            estatisticas_canais ec 
        WHERE 
            assunto = 'cities skylines'
            AND id_canal = 'UCrOH1V-FyMunBIMrKL0y0xQ'

        ORDER BY 
            data_extracao ASC

        """
    resultado = conn.execute(sql)
    print(type(resultado))
    for linha in resultado:
        print(linha)
