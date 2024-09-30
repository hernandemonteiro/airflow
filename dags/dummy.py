from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    'dummy',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:

    tarefa_1 = EmptyOperator(task_id='tarefa_1')
    tarefa_2 = EmptyOperator(task_id='tarefa_2')
    tarefa_3 = EmptyOperator(task_id='tarefa_3')
    tarefa_4 = BashOperator(
        task_id='cria_pasta_dummy',
        bash_command='mkdir -p "/home/darkhorse/Desktop/ssd_2/codes/airflow/pasta_dummy"',  # pylint: disable=C0301 # noqa
    )

    tarefa_1 >> [tarefa_2, tarefa_3]
    tarefa_3 >> tarefa_4
