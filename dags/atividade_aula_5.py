import os
from os.path import join

import pandas as pd  # type: ignore
import pendulum
from dotenv import load_dotenv  # type: ignore

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add


with DAG(
    'atividade_aula_5',
    start_date=pendulum.datetime(2024, 8, 22, tz="UTC"),
        schedule_interval='0 0 * * 1',
) as dag:

    tarefa_1 = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p "/home/darkhorse/Desktop/ssd_2/codes/airflow/data/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'  # pylint: disable=C0301 # noqa
    )

    def fazer_requisicao(data_fim: str):
        load_dotenv()

        city = 'Boston'
        key = os.environ['API_KEY']

        url = join('https://weather.visualcrossing.com/',
                   'VisualCrossingWebServices/rest/services/timeline/',
                   f'{city}/{data_fim}/{ds_add(data_fim, 7)}',
                   f'?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(url)
        file_path = f"./data/semana={data_fim}/"

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(
            file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(
            file_path + 'condicoes.csv')

    tarefa_2 = PythonOperator(
        task_id='tarefa_2',
        python_callable=fazer_requisicao,
        op_kwargs={
            'data_fim': '{{data_interval_end.strftime("%Y-%m-%d")}}'
        }
    )

    tarefa_1 >> tarefa_2
