from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago

# Configurações da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Definição da DAG
with DAG(
    'criar_tabela_hive',
    default_args=default_args,
    description='Criação de uma tabela no Apache Hive',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Query HiveQL para criação da tabela
    create_table_hive = """
    CREATE TABLE IF NOT EXISTS youtube.my_table (
        id INT,
        name STRING,
        age INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;
    """

    criar_tabela = HiveOperator(
        task_id='test_hive_connection_task',
        hql=create_table_hive,
        hive_cli_conn_id='hiveserver2_default',  # ID da conexão Hive
        dag=dag,
    )

    # Definir a sequência de execução
    criar_tabela
