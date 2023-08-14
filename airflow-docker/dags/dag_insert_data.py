from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import requests
from bs4 import BeautifulSoup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# Definir a URL de origem e criar e puxar data de atualização dos dados
SOURCE_URL = 'https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta'
REQUISICAO = requests.get(SOURCE_URL)
SITE = BeautifulSoup(REQUISICAO.text, 'lxml')
LAST_UPDATE_SOURCE = SITE.find('span', {'class': 'documentModified'}).find(
    'span', {'class': 'value'}).text.replace('h', ':')


def criar_df_atualizacao():
    now_utc = datetime.utcnow()
    now_utc_minus_3_hours = now_utc - timedelta(hours=3)

    CARGA = {
        'Ultima_Atualizacao': [LAST_UPDATE_SOURCE],
        'Ultima_Carga': [now_utc_minus_3_hours.strftime('%d/%m/%Y %H:%M:%S')]
    }
    dfCarga = pd.DataFrame(CARGA)
    return dfCarga


# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

with DAG(
    dag_id='dag_carga_atualizacao',
    default_args=default_args,
    schedule_interval=None,
) as dag:
    task1 = PythonOperator(
        task_id='Criar_DF',
        python_callable=criar_df_atualizacao,
        provide_context=True,
    )

    task2 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            drop table if exists dag_atualizacao;
            create table if not exists dag_atualizacao (
                Ultima_Atualizacao VARCHAR(255),
                Ultima_Carga VARCHAR(255),
                primary key (Ultima_Atualizacao, Ultima_Carga)
            );
        """
    )

    task3 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_atualizacao (Ultima_Atualizacao, Ultima_Carga)
            values ('{{ ti.xcom_pull(task_ids='Criar_DF')['Ultima_Atualizacao'][0] }}',
                    '{{ ti.xcom_pull(task_ids='Criar_DF')['Ultima_Carga'][0] }}')
        """
    )


task1 >> task2 >> task3
