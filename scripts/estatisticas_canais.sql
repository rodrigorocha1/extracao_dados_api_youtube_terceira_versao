/*
 * 
 * Canais: 'UCCe4Be21OPPltTTIHP4lDvg'
 */
# consulta geral

SELECT *
from estatisticas_canais ec 
where ec.assunto = 'cities skylines';


# Total vísualizações 
SELECT
	data_extracao,
	nm_canal,
	turno_extracao,
	total_videos_publicados,
	id_canal
FROM
	estatisticas_canais ec
WHERE
	assunto = 'cities skylines'
	AND id_canal = 'UCccdWOiGy2vEQKjUhNZHB_g' -- AND turno_extracao = 'Noite'
ORDER BY
	data_extracao ASC;

   
   SELECT *
   from estatisticas_canais;
  
1- # Total vísualizações/inscritos/videos_publicados turno canal

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
	total_videos_publicados,
	case
		when total_videos_publicados - (
			LAG(total_videos_publicados, 1) OVER(
				PARTITION BY id_canal
				ORDER BY
					data_extracao
			)
		) IS NULL then 0
		else total_videos_publicados - (
			LAG(total_videos_publicados, 1) OVER(
				PARTITION BY id_canal
				ORDER BY
					data_extracao
			)
		)
	end as total_videos_publicados_turno
FROM
	estatisticas_canais
WHERE
	assunto = 'cities skylines'
	AND id_canal = 'UCrOH1V-FyMunBIMrKL0y0xQ'
    
   
ORDER BY 
    data_extracao ASC;

   
   
   

   
   
 #2 - Total vísualizações/inscritos/videos_publicado por dia

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
	nm_canal,
	turno_extracao,
	total_videos_publicados,
	COALESCE (
		LAG(total_videos_publicados, 1) OVER(
			PARTITION BY id_canal
			ORDER BY
				data_extracao
		),
		0
	) AS visualizacoes_anterior,
	COALESCE (
		total_videos_publicados - LAG(total_videos_publicados, 1) OVER(
			PARTITION BY id_canal
			ORDER BY
				data_extracao
		),
		0
	) as total_visualizacoes_dia
FROM
	estatisticas_canais ec
WHERE
	assunto = 'cities skylines'
	AND id_canal = 'UCrOH1V-FyMunBIMrKL0y0xQ'
	AND turno_extracao = 'Noite'
ORDER BY
	data_extracao ASC;
   
   
   
 ================DADOS VÌDEO =======================
 

SELECT
	DISTINCT ec.id_canal as id_canal,
	ec.nm_canal as nm_canal,
	ec.assunto as assunto
from
	estatisticas_canais ec;
SELECT
	*
from
	estatisticas_videos ev
where
	ev.assunto = 'cities skylines'
	and ev.id_video = 'jU_ooxfchd4';



# 3-  TOTAL Visualizações, total comentários e total_likes vídeo Turno 
SELECT 
	id_video,
	titulo_video 
from depara_video dv 
WHERE dv.assunto = ;

SELECT 
	id_canal,
	nm_canal
	
	
from depara_canais  
where assunto = ;


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
			ev.total_visualizacoes - LAG(ev.total_visualizacoes, 1) OVER (
				PARTITION BY id_canal
				ORDER BY
					data_extracao
			),
			0
		) = 0 THEN ev.total_visualizacoes
		ELSE COALESCE(
			ev.total_visualizacoes - LAG(ev.total_visualizacoes, 1) OVER (
				PARTITION BY id_canal
				ORDER BY
					data_extracao
			),
			0
		)
	END AS total_visualizacoes_turno
FROM
	estatisticas_videos ev
WHERE
	ev.assunto = 'cities skylines'
	AND ev.id_video = 'jU_ooxfchd4'



# 4 - Total Visualizações, total comentários e total_likes  vídeo dia 



SELECT
	ev.data_extracao as data_extracao,
	ev.titulo_video,
	ev.total_visualizacoes,
	LAG(ev.total_visualizacoes, 1) OVER(
		PARTITION BY id_canal
		ORDER BY
			data_extracao
	) AS total_videos_publicados_anterior,
	CASE
		when coalesce(
			ev.total_visualizacoes - LAG(ev.total_visualizacoes, 1) OVER(
				PARTITION BY id_canal
				ORDER BY
					data_extracao
			),
			0
		) = 0 then ev.total_visualizacoes
		else coalesce(
			ev.total_visualizacoes - LAG(ev.total_visualizacoes, 1) OVER(
				PARTITION BY id_canal
				ORDER BY
					data_extracao
			),
			0
		)
	end as total_visualizacoes_turno
from
	estatisticas_videos ev
where
	ev.assunto = 'cities skylines'
	and ev.id_video = 'jU_ooxfchd4'
	And ev.turno_extracao = 'Noite';
-- Engajamento dia

 
SELECT 
	ROUND(((ev.total_likes + ev.total_comentarios ) / ev.total_visualizacoes) * 100, 2) as taxa_engajamento
from estatisticas_videos ev 
where ev.assunto = 'cities skylines'
and ev.id_video = 'jU_ooxfchd4'
and ev.turno_extracao  = 'Noite';



-- 5 - Média da taxa de engajamento do vídeo por dia

SELECT
	ev.id_video,
	ev.dia_extracao,
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
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / ev.total_visualizacoes
				) * 100
			),
			2
		),
		0
	) as media_taxa_engajamento
from
	estatisticas_videos ev
where
	ev.assunto = 'cities skylines'
	AND id_video IN ('BDzwY2A4KPM', 'jU_ooxfchd4')
GROUP BY
	ev.id_video,
	ev.dia_extracao,
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
	)
HAVING
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / ev.total_visualizacoes
				) * 100
			),
			2
		),
		0
	) > 0
ORDER BY
	2;
-- and ev.turno_extracao  = 'Noite';

SELECT *
from depara_video dv 
where dv.id_video  in ('EvxKsuwWblg')



SELECT
	ev.id_video,
	dvv.titulo_video,
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
	) AS dia_da_semana,
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / ev.total_visualizacoes
				) * 100
			),
			2
		),
		0
	) as media_taxa_engajamento
from
	estatisticas_videos ev
	INNER JOIN (
		SELECT
			*
		FROM
			depara_video dv
		WHERE
			dv.assunto = 'cities skylines'
			AND dv.id_video IN ('BDzwY2A4KPM', 'jU_ooxfchd4')
	) dvv on dvv.id_video = ev.id_video
where
	ev.assunto = 'cities skylines'
	AND ev.id_video IN ('BDzwY2A4KPM', 'jU_ooxfchd4')
GROUP BY
	ev.id_video,
	dvv.titulo_video,
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
	),
	dayofweek(ev.data_extracao)
HAVING
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / ev.total_visualizacoes
				) * 100
			),
			2
		),
		0
	) > 0
ORDER BY
	dayofweek(ev.data_extracao);





---- 6 -Média engajamento  do  canal por visualização


SELECT *
from depara_canais dc 
where dc.id_canal  in ('UCrOH1V-FyMunBIMrKL0y0xQ', 'UCCe4Be21OPPltTTIHP4lDvg');


SELECT
	ev.id_canal,
	dcc.nm_canal,
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
	) AS dia_da_semana,
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / ev.total_visualizacoes
				) * 100
			),
			2
		),
		0
	) as media_taxa_engajamento
from
	estatisticas_videos ev
	INNER JOIN (
		SELECT
			*
		FROM
			depara_canais dc
		WHERE
			dc.assunto = 'cities skylines'
			AND dc.id_canal in (
				'UCrOH1V-FyMunBIMrKL0y0xQ',
				'UCCe4Be21OPPltTTIHP4lDvg'
			)
	) dcc on dcc.id_canal = ev.id_canal
where
	ev.assunto = 'cities skylines'
	AND ev.id_canal in (
		'UCrOH1V-FyMunBIMrKL0y0xQ',
		'UCCe4Be21OPPltTTIHP4lDvg'
	)
GROUP BY
	ev.id_canal,
	dcc.nm_canal,
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
	),
	dayofweek(ev.data_extracao)
HAVING
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / ev.total_visualizacoes
				) * 100
			),
			2
		),
		0
	) > 0
ORDER BY
	dayofweek(ev.data_extracao);






--  7 - média taxa engajamento do canal por total de inscritos
 
SELECT *
from estatisticas_canais ec ;

SELECT *
from estatisticas_videos ev 
where ev.id_canal  in ('UCrOH1V-FyMunBIMrKL0y0xQ', 'UCCe4Be21OPPltTTIHP4lDvg');

SELECT
	ev.id_canal,
	dcc.nm_canal,
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
	) AS dia_da_semana,
	dayofweek(ev.data_extracao) as dia_semana,
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / dcc.total_inscritos
				) * 100
			),
			2
		),
		0
	) as media_taxa_engajamento
FROM
	estatisticas_videos ev
	INNER JOIN (
		SELECT
			ec.total_inscritos as total_inscritos,
			ec.id_canal as id_canal,
			ec.nm_canal as nm_canal
		FROM
			estatisticas_canais ec
		WHERE
			ec.assunto = 'cities skylines'
			AND ec.id_canal in (
				'UCrOH1V-FyMunBIMrKL0y0xQ',
				'UCCe4Be21OPPltTTIHP4lDvg'
			)
	) dcc on dcc.id_canal = ev.id_canal
WHERE
	ev.assunto = 'cities skylines'
	AND ev.id_canal in (
		'UCrOH1V-FyMunBIMrKL0y0xQ',
		'UCCe4Be21OPPltTTIHP4lDvg'
	)
GROUP BY
	ev.id_canal,
	dcc.nm_canal,
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
	),
	dayofweek(ev.data_extracao)
HAVING
	COALESCE(
		ROUND(
			AVG(
				(
					(ev.total_likes + ev.total_comentarios) / ev.total_visualizacoes
				) * 100
			),
			2
		),
		0
	) > 0
ORDER BY
	4;


WITH canal_info AS (
	SELECT
		ec.id_canal,
		ec.nm_canal,
		ec.total_inscritos
	FROM
		estatisticas_canais ec
	WHERE
		ec.assunto = 'cities skylines'
		AND ec.id_canal IN (
			'UCrOH1V-FyMunBIMrKL0y0xQ',
			'UCCe4Be21OPPltTTIHP4lDvg'
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
		ev.assunto = 'cities skylines'
		AND ev.id_canal IN (
			'UCrOH1V-FyMunBIMrKL0y0xQ',
			'UCCe4Be21OPPltTTIHP4lDvg'
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
	vi.dia_semana;
SELECT CURRENT_TIMESTAMP


SELECT DISTINCT  dc.assunto 
from depara_canais dc 


-- 8 Fequência de vídeos publicados por assunto





SELECT
	dia_da_semana,
	total_videos,
	total_videos_publicados_dia
FROM
	(
		SELECT
			CASE
				date_format(ec.data_extracao, 'EEEE')
				WHEN 'Monday' THEN 'Segunda-feira'
				WHEN 'Tuesday' THEN 'Terça-feira'
				WHEN 'Wednesday' THEN 'Quarta-feira'
				WHEN 'Thursday' THEN 'Quinta-feira'
				WHEN 'Friday' THEN 'Sexta-feira'
				WHEN 'Saturday' THEN 'Sábado'
				WHEN 'Sunday' THEN 'Domingo'
			END AS dia_da_semana,
			SUM(ec.total_videos_publicados) AS total_videos,
			COALESCE(
				SUM(ec.total_videos_publicados) - LAG(SUM(ec.total_videos_publicados)) OVER (
					ORDER BY
						MIN(DAYOFWEEK(ec.data_extracao))
				),
				0
			) AS total_videos_publicados_dia
		FROM
			estatisticas_canais ec
		WHERE
			ec.assunto = 'cities skylines'
			and ec.turno_extracao = 'Noite'
		GROUP BY
			CASE
				date_format(ec.data_extracao, 'EEEE')
				WHEN 'Monday' THEN 'Segunda-feira'
				WHEN 'Tuesday' THEN 'Terça-feira'
				WHEN 'Wednesday' THEN 'Quarta-feira'
				WHEN 'Thursday' THEN 'Quinta-feira'
				WHEN 'Friday' THEN 'Sexta-feira'
				WHEN 'Saturday' THEN 'Sábado'
				WHEN 'Sunday' THEN 'Domingo'
			END
	) AS subquery -- Filtrar para valores onde total_videos_publicados_dia > 0
WHERE
	total_videos_publicados_dia > 0
ORDER BY
	FIELD(
		dia_da_semana,
		'Domingo',
		'Segunda-feira',
		'Terça-feira',
		'Quarta-feira',
		'Quinta-feira',
		'Sexta-feira',
		'Sábado'
	);
   
   

   
   
   
SELECT 
        CASE date_format(ec.data_extracao, 'EEEE')
            WHEN 'Monday' THEN 'Segunda-feira'
            WHEN 'Tuesday' THEN 'Terça-feira'
            WHEN 'Wednesday' THEN 'Quarta-feira'
            WHEN 'Thursday' THEN 'Quinta-feira'
            WHEN 'Friday' THEN 'Sexta-feira'
            WHEN 'Saturday' THEN 'Sábado'
            WHEN 'Sunday' THEN 'Domingo'
        END AS dia_da_semana,
        SUM(ec.total_videos_publicados) AS total_videos,

        COALESCE(
            SUM(ec.total_videos_publicados) - 
            LAG(SUM(ec.total_videos_publicados)) 
            OVER (ORDER BY MIN(DAYOFWEEK(ec.data_extracao))), 
        0) AS total_videos_publicados_dia

    FROM estatisticas_canais ec 
    WHERE ec.assunto = 'cities skylines'
    and ec.turno_extracao = 'Noite'

    GROUP BY 
        CASE date_format(ec.data_extracao, 'EEEE')
            WHEN 'Monday' THEN 'Segunda-feira'
            WHEN 'Tuesday' THEN 'Terça-feira'
            WHEN 'Wednesday' THEN 'Quarta-feira'
            WHEN 'Thursday' THEN 'Quinta-feira'
            WHEN 'Friday' THEN 'Sexta-feira'
            WHEN 'Saturday' THEN 'Sábado'
            WHEN 'Sunday' THEN 'Domingo'
        END



-------------------
SELECT data_extracao, regexp_replace( date_format(data_extracao, 'EEEE'), 'Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday', CASE date_format(data_extracao, 'EEEE') WHEN 'Monday' THEN 'Segunda-feira' WHEN 'Tuesday' THEN 'Terça-feira' WHEN 'Wednesday' THEN 'Quarta-feira' WHEN 'Thursday' THEN 'Quinta-feira' WHEN 'Friday' THEN 'Sexta-feira' WHEN 'Saturday' THEN 'Sábado' WHEN 'Sunday' THEN 'Domingo' END ) AS dia_da_semana, total_visualizacoes, COALESCE ( LAG(total_visualizacoes, 1) OVER( PARTITION BY id_canal ORDER BY data_extracao ), 0 ) AS total_visualizacoes_anterior, COALESCE ( total_visualizacoes - LAG(total_visualizacoes, 1) OVER( PARTITION BY id_canal ORDER BY data_extracao ), 0 ) as total_visualizacoes_dia FROM estatisticas_canais ec WHERE assunto = 'Linux' AND id_canal = 'UCbqbbDSvjo4kmwIITnXbJag' AND turno_extracao = 'Noite' ORDER BY data_extracao ASC