from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import DAG
import pendulum

with DAG(
    dag_id='teste_dag_spark',

    schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
) as dag:
    transformacao_teste = SparkSubmitOperator(
        task_id='spark_transformacao_dados_canais',
        conn_id='spark_default',  # Especifica o conn_id para o Spark
        application="/home/rodrigo/Documentos/projetos/extracao_dados_api_youtube/spark_etl/teste_spark.py",
        application_args=None,
    )

    transformacao_teste
