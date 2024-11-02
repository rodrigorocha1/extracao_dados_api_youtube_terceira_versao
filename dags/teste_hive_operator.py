from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago

# Defina os parâmetros da DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    dag_id='test_hive_connection',
    default_args=default_args,
    description='DAG para testar a conexão com o Apache Hive',
    schedule_interval='@once',  # Execute uma vez
    start_date=days_ago(1),
)

# Tarefa para executar uma consulta simples no Hive
test_connection = HiveOperator(
    task_id='test_hive_connection_task',
    hql='SELECT current_date();',
    hive_cli_conn_id='hive_conn',  # ID da conexão Hive
    dag=dag,
)

test_connection
