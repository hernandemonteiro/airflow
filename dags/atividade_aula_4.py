from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def cumprimentos():
    print("Boas-vindas ao Airflow!")


with DAG(
    'atividade_aula_4',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:

    tarefa_1 = EmptyOperator(task_id='tarefa_1')
    tarefa_2 = PythonOperator(
        task_id='tarefa_2',
        python_callable=cumprimentos,
    )
    tarefa_1 >> tarefa_2
