from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('exemplo_jinja', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:

    bash_task = BashOperator(
        task_id="print_day_of_week",
        bash_command="echo Today is {{ execution_date.in_tz('America/Sao_Paulo').strftime('%Y-%m-%dT%H:%M:%SZ') }}",
    )
    bash_task
