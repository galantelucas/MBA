from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import requests
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from io import StringIO


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


def drop_lines_2022():
    conn_id = 'postgres_localhost'
    hook = PostgresHook(postgres_conn_id=conn_id)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            drop_lines_22 = """
                DELETE FROM TBL_IMPORTACAO WHERE CO_ANO = 2022;
            """

            cursor.execute(drop_lines_22)
            conn.commit()


def drop_lines_2023():
    conn_id = 'postgres_localhost'
    hook = PostgresHook(postgres_conn_id=conn_id)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            drop_lines_23 = """
                DELETE FROM TBL_IMPORTACAO WHERE CO_ANO = 2023;
            """

            cursor.execute(drop_lines_23)
            conn.commit()


def create_postgres_table():
    conn_id = 'postgres_localhost'
    hook = PostgresHook(postgres_conn_id=conn_id)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            create_table_sql = """
                            CREATE TABLE IF NOT EXISTS TBL_IMPORTACAO (
                CO_ANO SMALLINT,
                CO_MES VARCHAR(2),
                CO_NCM VARCHAR(8),
                CO_UNID VARCHAR(2),
                CO_PAIS VARCHAR(3),
                SG_UF_NCM VARCHAR(2),
                CO_VIA VARCHAR(2),
                CO_URF VARCHAR(7),
                QT_ESTAT DECIMAL(18, 4),
                KG_LIQUIDO DECIMAL(18, 4),
                VL_FOB DECIMAL(18, 4),
                VL_FRETE DECIMAL(18, 4),
                VL_SEGURO DECIMAL(18, 4),
                MOVIMENTACAO VARCHAR(10)
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()


def download_csv_and_create_df(url):
    response = requests.get(url, verify=False)
    csv_content = response.text
    df = pd.read_csv(StringIO(csv_content), sep=';', encoding='latin-1')
    df['MOVIMENTACAO'] = 'IMPORTACAO'
    return df


def decide_which_path(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    sql_query = "SELECT count(co_ano) FROM TBL_IMPORTACAO WHERE CO_ANO = 2022"
    row_count_2022 = pg_hook.get_first(sql_query)[0]

    df_task1 = kwargs['ti'].xcom_pull(task_ids='download_csv_imp_2022')

    if row_count_2022 == len(df_task1):
        return 'drop_2023'
    else:
        return 'drop_2022'


def insert_imp_2023_data(**kwargs):
    conn_id = 'postgres_localhost'
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        df_task2 = kwargs['ti'].xcom_pull(task_ids='download_csv_imp_2023')

        if df_task2 is not None and not df_task2.empty:
            data_to_insert = df_task2.to_records(index=False).tolist()

            insert_sql = """
                
                INSERT INTO TBL_IMPORTACAO (CO_ANO, CO_MES, CO_NCM, CO_UNID, CO_PAIS, SG_UF_NCM, CO_VIA, CO_URF, QT_ESTAT, KG_LIQUIDO, VL_FOB, VL_FRETE, VL_SEGURO, MOVIMENTACAO)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.executemany(insert_sql, data_to_insert)
            conn.commit()

    finally:
        cursor.close()
        conn.close()


def insert_imp_2022_data(**kwargs):
    conn_id = 'postgres_localhost'
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        df_task1 = kwargs['ti'].xcom_pull(task_ids='download_csv_imp_2022')

        if df_task1 is not None and not df_task1.empty:
            data_to_insert = df_task1.to_records(index=False).tolist()

            insert_sql = """
                
                INSERT INTO TBL_IMPORTACAO (CO_ANO, CO_MES, CO_NCM, CO_UNID, CO_PAIS, SG_UF_NCM, CO_VIA, CO_URF, QT_ESTAT, KG_LIQUIDO, VL_FOB, VL_FRETE, VL_SEGURO, MOVIMENTACAO)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.executemany(insert_sql, data_to_insert)
            conn.commit()

    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id='dag_insert_importacao',
    default_args=default_args,
    schedule_interval='@once',
) as dag:
    task1 = PythonOperator(
        task_id='download_csv_imp_2022',
        python_callable=download_csv_and_create_df,
        op_args=[
            "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2022.csv"],
    )

    task2 = PythonOperator(
        task_id='download_csv_imp_2023',
        python_callable=download_csv_and_create_df,
        op_args=[
            "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_2023.csv"],
    )

    task3 = PythonOperator(
        task_id='create_postgres_table',
        python_callable=create_postgres_table,
        dag=dag,
    )

    branch_task = BranchPythonOperator(
        task_id='decide_branch',
        python_callable=decide_which_path,

        dag=dag,
    )

    drop_2022 = PythonOperator(
        task_id='drop_2022',
        python_callable=drop_lines_2022,

        dag=dag,
    )

    drop_2023 = PythonOperator(
        task_id='drop_2023',
        python_callable=drop_lines_2023,

        dag=dag,
    )

    insert_imp_2023 = PythonOperator(
        task_id='insert_imp_2023',
        python_callable=insert_imp_2023_data,

        dag=dag,
    )

    insert_imp_2022 = PythonOperator(
        task_id='insert_imp_2022',
        python_callable=insert_imp_2022_data,

        dag=dag,
    )

    end_task = DummyOperator(
        task_id='end_task',
        dag=dag,
    )

    task3 >> [
        task1, task2] >> branch_task >> drop_2023 >> insert_imp_2023 >> end_task
    task3 >> [task1, task2] >> branch_task >> drop_2022 >> insert_imp_2022 >> drop_2023 >> insert_imp_2023 >> end_task
