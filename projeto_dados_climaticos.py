from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import pendulum
import pandas as pd

with DAG(
    "projeto_dados_climaticos",
    start_date=pendulum.datetime(2023, 6, 26, tz="UTC"),
    schedule_interval='0 0 * * 1', #Executa toda segunda-feira
) as dag:
    
    def extrai_dados(data_interval_end):

        city = 'Belo_Horizonte'
        key = 'WESCWP83G2GU3A6GCQAWYZF5M'

        URL = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv'

        dados = pd.read_csv(URL)

        file_path = f'/home/rafael/Documents/airflowalura/Clima_Semana_={data_interval_end}/'

        dados.to_csv(file_path+'dados_brutos.csv')
        dados[["datetime", "tempmin", "temp", "tempmax"]].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description','icon']].to_csv(file_path + 'condicoes.csv')

    tarefa_1 = BashOperator(
        task_id = 'exclui_diretorios_antigos',
        bash_command='find /home/rafael/Documents/airflowalura/ -type d -name "Clima_Semana_={{data_interval_end.strftime("%Y-%m-%d")}}" -exec rm -rf {} +'
    )

    tarefa_2 = BashOperator(
        task_id = 'cria_pasta',
        bash_command='mkdir "/home/rafael/Documents/airflowalura/Clima_Semana_={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    tarefa_3= PythonOperator(
        task_id = 'Extracao_dados',
        python_callable = extrai_dados,
        op_kwargs= {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2 >> tarefa_3 