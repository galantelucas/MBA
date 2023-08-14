from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import requests
import pandas as pd
from io import StringIO
from contextlib import closing

# Função para fazer o download e criar o DataFrame


def download_and_read_csv(url, columns_to_select):
    response = requests.get(url, verify=False)
    csv_content = response.text
    df = pd.read_csv(StringIO(csv_content), sep=';',
                     usecols=columns_to_select, index_col=False)
    return df


# Dicionário com as URLs e as colunas que você deseja selecionar
data_sources = {
    "PAIS": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv",
        "columns": ['CO_PAIS', 'NO_PAIS']
    },
    "URF": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv",
        "columns": ['CO_URF', 'NO_URF']
    },
    "VIA": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv",
        "columns": ['CO_VIA', 'NO_VIA']
    },
    "NCM": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
        "columns": ['CO_NCM', 'CO_UNID', 'CO_SH6', 'CO_ISIC_CLASSE', 'NO_NCM_POR']
    },
    "NCM_SH": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_SH.csv",
        "columns": ['CO_SH6', 'NO_SH6_POR', 'CO_SH4', 'NO_SH4_POR', 'CO_SH2', 'NO_SH2_POR']
    },
    "UNIDADE": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_UNIDADE.csv",
        "columns": ['CO_UNID', 'NO_UNID', 'SG_UNID']
    },
    "UF": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/UF.csv",
        "columns": ['CO_UF', 'SG_UF', 'NO_UF', 'NO_REGIAO']
    },
    "BLOCOS": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv",
        "columns": ['CO_PAIS', 'CO_BLOCO', 'NO_BLOCO']
    },
    "NCM_ISIC": {
        "url": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_ISIC.csv",
        "columns": ['CO_ISIC_CLASSE', 'NO_ISIC_CLASSE', 'CO_ISIC_SECAO', 'NO_ISIC_SECAO']
    }

}

# Função para fazer a carga dos DataFrames no banco de dados PostgreSQL


def load_data_to_postgres(df, table_name):
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/test')

    try:
        with closing(engine.connect()) as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False)
    finally:
        if engine:
            engine.dispose()


# Argumentos padrão da DAG
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

# Criação da DAG
dag = DAG('dag_tabelas_auxiliares',
          default_args=default_args,
          catchup=False,  # Evita a execução de tarefas para trás
          schedule_interval='@once')  # Define para que a execução seja manual

# Função para carregar dados no banco de dados


def load_data(**kwargs):
    for name, source in data_sources.items():
        df = download_and_read_csv(source["url"], source["columns"])
        table_name = f"{name.upper()}"
        load_data_to_postgres(df, table_name)


# Tarefa para carregar os dados no banco de dados
task_load_data = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data,
    provide_context=True,  # Adicione esse argumento para obter o contexto
    dag=dag
)
# Tarefa vazia como "trigger" para iniciar a execução manual
task_trigger = PythonOperator(
    task_id='trigger_execution',
    python_callable=lambda: None,
    dag=dag
)

# Defina as dependências entre as tarefas
task_trigger >> task_load_data
