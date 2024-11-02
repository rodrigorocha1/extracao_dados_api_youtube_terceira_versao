import pendulum
import argparse
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import os


def abrir_dataframe(spark: SparkSession, caminho: str) -> DataFrame:
    dataframe = spark.read.json(caminho)
    return dataframe


def obter_turno(data_extracao_col: F.col):
    return F.when((F.hour(data_extracao_col) >= 0) & (F.hour(data_extracao_col) < 6), 'Madrugada') \
            .when((F.hour(data_extracao_col) >= 6) & (F.hour(data_extracao_col) < 12), 'Manhã') \
            .when((F.hour(data_extracao_col) >= 12) & (F.hour(data_extracao_col) < 18), 'Tarde') \
            .otherwise('Noite')


def fazer_tratamento_canais(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.select(
        'data_extracao',
        'assunto',
        F.explode('items').alias('items')
    ).select(
        F.col('data_extracao').alias('DATA_EXTRACAO'),
        F.col('items.snippet.title').alias('NM_CANAL').cast('string'),
        F.col('items.statistics.subscriberCount').alias(
            'TOTAL_INSCRITOS').cast('integer'),
        F.col('items.statistics.videoCount').alias(
            'TOTAL_VIDEOS_PUBLICADOS').cast('integer'),
        F.col('items.statistics.viewCount').alias(
            'TOTAL_VISUALIZACOES').cast('integer'),

        F.year('data_extracao').alias('ANO_EXTRACAO').cast('integer'),
        F.month('data_extracao').alias('MES_EXTRACAO').cast('integer'),
        F.dayofmonth('data_extracao').alias('DIA_EXTRACAO').cast('integer'),
        obter_turno(F.col('data_extracao')).alias('TURNO_EXTRACAO'),
        F.col('items.id').alias('ID_CANAL').cast('string'),
        F.col('assunto').alias('ASSUNTO').cast('string'),
    )

    return dataframe


def fazer_tratamento_video(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.select('data_extracao', 'assunto', F.explode('items').alias('items')) \
        .select(

        F.col('data_extracao').alias('DATA_EXTRACAO'),
        F.col('items.snippet.title').alias('TITULO_VIDEO').cast('string'),
        F.col('items.snippet.description').alias('DESCRICAO').cast('string'),
        F.col('items.contentDetails.duration').alias('DURACAO'),
        F.col('items.snippet.tags').alias('TAGS'),
        F.col('items.snippet.categoryid').alias('ID_CATEGORIA'),
        F.col('items.statistics.viewCount').alias(
            'TOTAL_VISUALIZACOES').cast('integer'),
        F.col('items.statistics.likeCount').alias(
            'TOTAL_LIKES').cast('integer'),
        F.col('items.statistics.favoriteCount').alias(
            'TOTAL_FAVORITOS').cast('integer'),
        F.col('items.statistics.commentCount').alias(
            'TOTAL_COMENTARIOS').cast('integer'),

        F.year('data_extracao').alias('ANO_EXTRACAO').cast('integer'),
        F.month('data_extracao').alias('MES_EXTRACAO').cast('integer'),
        F.dayofmonth('data_extracao').alias('DIA_EXTRACAO').cast('integer'),
        obter_turno(F.col('data_extracao')).alias('TURNO_EXTRACAO'),
        F.col('assunto').alias('ASSUNTO').cast('string'),
        F.col('items.id').alias('ID_VIDEO').cast('string'),
        F.col('items.snippet.channelId').alias('ID_CANAL').cast('string')
    )
    dataframe = dataframe.withColumn('TOTAL_TAGS', F.when(
        F.size(dataframe.TAGS) <= 0, 0).otherwise(F.size(dataframe.TAGS)).cast('integer'))
    dataframe = dataframe.withColumn(
        'TOTAL_PALAVRAS_TITULO', F.size(F.split(dataframe.TITULO_VIDEO, " ")).cast('integer'))
    dataframe = dataframe.withColumn(
        'TOTAL_PALAVRAS_DESCRICAO', F.size(F.split(dataframe.DESCRICAO, " ")).cast('integer'))

    dataframe = dataframe.select(
        'DATA_EXTRACAO',
        'TITULO_VIDEO',
        'DESCRICAO',
        'DURACAO',
        'TAGS',
        'ID_CATEGORIA',
        'TOTAL_VISUALIZACOES'
        'TOTAL_LIKES',
        'TOTAL_FAVORITOS',
        'TOTAL_COMENTARIOS',
        'TOTAL_TAGS',
        'TOTAL_PALAVRAS_TITULO',
        'TOTAL_PALAVRAS_DESCRICAO',
        'ANO_EXTRACAO',
        'MES_EXTRACAO',
        'DIA_EXTRACAO',
        'TURNO_EXTRACAO',
        'ASSUNTO',
        'ID_VIDEO',
        'ID_CANAL'
    )
    return dataframe


def salvar_dados_particionados(dataframe: DataFrame, caminho_completo: str):
    print(dataframe.show())
    dataframe.write.mode("append").parquet(caminho_completo)


if __name__ == "__main__":
    print('______iniciando______________')
    caminho_base = os.getcwd()
    parser = argparse.ArgumentParser(
        description='ETL YOUTUBE')
    parser.add_argument('--opcao', type=str, required=True,
                        help='Opcao para obter a métrica')
    parser.add_argument('--path_extracao', type=str, required=True,
                        help='camihno do pasta data')
    parser.add_argument('--path_metrica', type=str, required=True,
                        help='camihno métrica')
    parser.add_argument('--path_arquivo', type=str, required=True,
                        help='camihno do arquivo')
    args = parser.parse_args()
    opcao = args.opcao
    path_extracao = args.path_extracao
    path_metrica = args.path_metrica
    path_arquivo = args.path_arquivo
    caminho_arquivo = f'/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/datalake_youtube/bronze/*/{path_extracao}/{path_metrica}/{path_arquivo}'

    # caminho_arquivo = '/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/datalake_youtube/bronze/assunto_power_bi/extracao_data_2024_11_02_11_49_manha/estatisticas_canais/req_canais.json'
    # opcao = 'C'
    spark = SparkSession.builder.appName("criar_dataframe").getOrCreate()

    dataframe = abrir_dataframe(
        spark, caminho_arquivo)

    if opcao == 'C':
        dataframe = fazer_tratamento_canais(dataframe)
        print('Dataframe Print Schema')
        print(dataframe.printSchema())
        caminho_arquivo = os.path.join(
            caminho_base, 'datalake_youtube', 'prata', 'estatisticas_canais')
        nome_arquivo = 'estatisticas_canais.parquet'
        particoes = "ASSUNTO", "ANO_EXTRACAO", "MES_EXTRACAO", "DIA_EXTRACAO", "TURNO_EXTRACAO"

    else:
        dataframe = fazer_tratamento_video(dataframe)
        caminho_arquivo = os.path.join(
            caminho_base, 'datalake_youtube', 'prata', 'estatisticas_videos')
        particoes = "ASSUNTO", "ANO_EXTRACAO", "MES_EXTRACAO", "DIA_EXTRACAO", "TURNO_EXTRACAO", "ID_CANAL", "ID_VIDEO"
        nome_arquivo = 'estatisticas_videos.parquet'
    caminho_completo = os.path.join(
        caminho_arquivo,  path_extracao, nome_arquivo)
    os.makedirs(caminho_arquivo, exist_ok=True)

    salvar_dados_particionados(
        dataframe=dataframe, caminho_completo=caminho_completo)
    spark.stop()
