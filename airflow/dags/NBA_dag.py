from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from bs4 import BeautifulSoup
import pandas as pd

seasons_url = {
    2024: "https://www.proballers.com/basketball/league/3/nba/standings",
    2023: "https://www.proballers.com/basketball/league/3/nba/standings/2022",
    2022: "https://www.proballers.com/basketball/league/3/nba/standings/2021",
    2021: "https://www.proballers.com/basketball/league/3/nba/standings/2020",
    2020: "https://www.proballers.com/basketball/league/3/nba/standings/2019"
}




def extract_data_frame(**kwargs):
    data_frames = {}
    for key, value in seasons_url.items():
        wep_page = requests.get(value)
        soup = BeautifulSoup(wep_page.content, 'html.parser')
        table = soup.find('div', class_="home-league__standings__content__tables__content")
        headers = [i.text for i in table.find_all('th')]
        df = pd.DataFrame(columns=headers)

        coloumns = table.find_all('tr')
        for i in coloumns[1:]:
            raw = [t.text.strip() for t in i.find_all('td')]
            index = len(df)
            df.loc[index] = raw
        data_frames[key] = df

    kwargs['ti'].xcom_push(key='data_frames', value=data_frames)

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data_frames', task_ids='extract_data')
    teams = list(data[2024]['Team'])
    teams_df = []
    for i in teams:
        data_test = pd.concat([data[2024][data[2024]['Team'] == i],
                               data[2023][data[2023]['Team'] == i],
                               data[2022][data[2022]['Team'] == i],
                               data[2021][data[2021]['Team'] == i],
                               data[2020][data[2020]['Team'] == i]],
                              axis=0)
        data_test = data_test.drop(columns='Team')
        index = pd.MultiIndex.from_product([[i], [2024, 2023, 2022, 2021, 2020]], names=['Team', 'Years'])
        df = pd.DataFrame(data_test.values, index=index, columns=data_test.columns)
        teams_df.append(df)

    data_teams_final = pd.concat(teams_df, axis=0)
    ti.xcom_push(key='data_teams_final', value=data_teams_final)





def load_the_file(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data_teams_final', task_ids='transform_data')
    file_path = r"F:\studying machine\airflow Dag using docker\data\data.csv"
    if os.path.exists(file_path):
        os.remove(file_path)
    data.to_csv(file_path, index=False)






default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 8),
    'email': ['youremail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'NPA_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_frame,
    provide_context=True,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_the_file,
    provide_context=True,
    dag=dag
)

extract_data_task >> transform_data_task >> load_data_task
