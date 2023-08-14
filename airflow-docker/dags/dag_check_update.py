from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import requests
from bs4 import BeautifulSoup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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


def compare_last_update_with_table(**kwargs):
    # Conectar ao banco de dados
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    # Consultar a tabela para obter o valor da última atualização
    sql_query = "SELECT Ultima_Atualizacao FROM dag_atualizacao ORDER BY Ultima_Carga ASC LIMIT 1"
    result = pg_hook.get_records(sql_query)

    if len(result) > 0:
        ultima_atualizacao_bd = result[0][0]
        ultima_atualizacao_df = kwargs['ti'].xcom_pull(
            task_ids='Criar_DF')['Ultima_Atualizacao'][0]

        # Comparar as datas
        if ultima_atualizacao_bd != ultima_atualizacao_df:
            return 'trigger_tabelas_auxiliares'
        else:
            return 'end_task'


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
    dag_id='dag_check_update',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='Criar_DF',
        python_callable=criar_df_atualizacao,
        provide_context=True,
    )

    decide_next = BranchPythonOperator(
        task_id='decide_next_task',
        python_callable=compare_last_update_with_table,
        provide_context=True,
        dag=dag,
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

    task5 = DummyOperator(
        task_id='end_task',
        dag=dag
    )

    # Criação do TriggerDagRunOperator para dag_tabelas_auxiliares
    trigger_tabelas_auxiliares = TriggerDagRunOperator(
        task_id='trigger_tabelas_auxiliares',
        trigger_dag_id='dag_tabelas_auxiliares',
        reset_dag_run=True,
        wait_for_completion=True
    )

    # Criação do TriggerDagRunOperator para dag_insert_exportacao
    trigger_insert_exportacao = TriggerDagRunOperator(
        task_id='trigger_insert_exportacao',
        trigger_dag_id='dag_insert_exportacao',
        reset_dag_run=True,
        wait_for_completion=True
    )

    # Criação do TriggerDagRunOperator para dag_insert_importacao
    trigger_insert_importacao = TriggerDagRunOperator(
        task_id='trigger_insert_importacao',
        trigger_dag_id='dag_insert_importacao',
        reset_dag_run=True,
        wait_for_completion=True
    )

    end_task_2 = DummyOperator(
        task_id='end_task_2',
        dag=dag,
    )

task1 >> decide_next
decide_next >> [trigger_tabelas_auxiliares, task5]
trigger_tabelas_auxiliares >> trigger_insert_exportacao >> trigger_insert_importacao >> task2 >> task3 >> end_task_2
