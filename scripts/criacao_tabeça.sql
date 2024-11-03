CREATE TABLE estatisticas_canais (
	DATA_EXTRACAO STRING,
	NM_CANAL STRING,
	TOTAL_INSCRITOS INT,
	TOTAL_VIDEOS_PUBLICADOS INT,
	TOTAL_VISUALIZACOES INT
	
) 
PARTITIONED BY (
	ANO_EXTRACAO INT,
	MES_EXTRACAO INT,
	DIA_EXTRACAO INT,
	TURNO_EXTRACAO STRING,
	ASSUNTO STRING,
	ID_CANAL STRING
	
	
)
STORED AS PARQUET;

use youtube;

DESCRIBE FORMATTED estatisticas_canais;

SELECT * FROM estatisticas_canais ec ;

: Input path does not exist: file:/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/datalake_youtube/prata/estatisticas_canais/extracao_data_2024_11_02_11_49_manha/estatisticas_canais.parquet

: Input path does not exist: file:/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/datalake_youtube/prata/estatisticas_canais/extracao_data_2024_11_02_11_49_manha/estatisticas_canais.parquet

LOAD DATA  INPATH '/opt/hive/prata/estatisticas_canais/extracao_data_2024_11_02_11_49_manha/estatisticas_canais.parquet/'
INTO TABLE estatisticas_canais;


LOAD DATA  INPATH '/opt/hive/prata/estatisticas_canais/extracao_data_2024_11_02_noite/estatisticas_canais.parquet/'
INTO TABLE estatisticas_canais ;


CREATE  TABLE estatisticas_videos (
	DATA_EXTRACAO string,
	TITULO_VIDEO string,
	DESCRICAO string,
	DURACAO string,
	TAGS ARRAY<string>,
	ID_CATEGORIA string,
	TOTAL_VISUALIZACOES INT, 
	TOTAL_LIKES INT,
	TOTAL_FAVORITOS INT,
	TOTAL_COMENTARIOS INT,
	TOTAL_TAGS INT,
	TOTAL_PALAVRAS_TITULO INT,
	TOTAL_PALAVRAS_DESCRICAO INT

) 
PARTITIONED BY(
	ANO_EXTRACAO INT, 
	MES_EXTRACAO INT, 
	DIA_EXTRACAO INT, 
	TURNO_EXTRACAO string,
	ASSUNTO VARCHAR(30),
	ID_VIDEO STRING,
	ID_CANAL STRING
	)
STORED AS PARQUET;




CREATE TABLE employee (
    name STRING,
    work_place ARRAY<STRING>,
    sex_age STRUCT<sex:STRING, age:INT>,
    skills_score MAP<STRING, INT>,
    depart_title MAP<STRING, ARRAY<STRING>>
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA  INPATH '/opt/hive/prata/estatisticas_videos/extracao_data_2024_11_02_noite/estatisticas_videos.parquet/'
        INTO TABLE estatisticas_videos

SELECT *
FROM estatisticas_videos;
 LOAD DATA  INPATH '/opt/hive/prata/estatisticas_videos/extracao_data_2024_11_02_noite/estatisticas_videos.parquet/'
        INTO TABLE estatisticas_videos

-- docker cp prata/ 957ec016ddf3:/opt/hive
