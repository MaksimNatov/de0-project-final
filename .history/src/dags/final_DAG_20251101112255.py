from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

def read_sql_file(file_path, **context):
    """Чтение SQL файла с подстановкой параметров"""
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    # Подстановка параметров
    sql_content = sql_content.format(
        yesterday=context['yesterday_ds'],
        ds=context['ds']
    )
    return sql_content

def get_sql_file_path(filename):
    """Получение абсолютного пути к SQL файлу"""
    # Поднимаемся на уровень выше из папки dags в корень airflow
    airflow_root = os.path.dirname(os.path.dirname(__file__))
    sql_path = os.path.join(airflow_root, 'sql', filename)
    return sql_path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_from_postgres(**context):
    """Извлечение данных из PostgreSQL"""
    yesterday = context['yesterday_ds']
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    vertica_hook = VerticaHook(vertica_conn_id='vertica_conn')
    
    # Очистка staging зоны (из файла)
    clear_sql_path = get_sql_file_path('clear_staging.sql')
    clear_sql = read_sql_file(clear_sql_path, **context)
    vertica_hook.run(clear_sql)
    
    # Загрузка транзакций из PostgreSQL
    transactions_query = f"""
        SELECT 
            operation_id,
            account_number_from,
            account_number_to,
            currency_code,
            country,
            status,
            transaction_type,
            amount,
            transaction_dt
        FROM public.transactions 
        WHERE transaction_dt::date = '{yesterday}'
          AND account_number_from >= 0 
          AND account_number_to >= 0
    """
    
    transactions_data = postgres_hook.get_records(transactions_query)
    
    # Вставка в Vertica
    if transactions_data:
        insert_transactions = """
            INSERT INTO STV230533__STAGING.transactions 
            (operation_id, account_number_from, account_number_to, currency_code, 
             country, status, transaction_type, amount, transaction_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        vertica_hook.insert_rows('STV230533__STAGING.transactions', transactions_data)
    
    # Загрузка курсов валют
    currencies_query = f"""
        SELECT 
            date_update,
            currency_code,
            currency_code_with,
            currency_with_div
        FROM public.currencies 
        WHERE date_update::date = '{yesterday}'
          AND currency_code_with = 420  -- USD
    """
    
    currencies_data = postgres_hook.get_records(currencies_query)
    
    if currencies_data:
        insert_currencies = """
            INSERT INTO STV230533__STAGING.currencies 
            (date_update, currency_code, currency_code_with, currency_with_div)
            VALUES (%s, %s, %s, %s)
        """
        vertica_hook.insert_rows('STV230533__STAGING.currencies', currencies_data)
    
    logger.info(f"Data extracted for date: {yesterday}")
    logger.info(f"Transactions loaded: {len(transactions_data)}")
    logger.info(f"Currencies loaded: {len(currencies_data)}")

def load_to_dwh(**context):
    """Загрузка данных в витрину DWH из SQL файла"""
    vertica_hook = VerticaHook(vertica_conn_id='vertica_conn')
    
    # Чтение SQL из файла
    merge_sql_path = get_sql_file_path('increment.sql')
    merge_sql = read_sql_file(merge_sql_path, **context)
    
    vertica_hook.run(merge_sql)
    logger.info(f"Data loaded to DWH for date: {context['yesterday_ds']}")

with DAG(
    'load_global_metrics_dag',
    default_args=default_args,
    description='DAG for loading global metrics from PostgreSQL to Vertica DWH',
    schedule_interval='0 1 * * *',  # Запуск каждый день в 1:00
    catchup=True,
    tags=['global_metrics', 'dwh']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_from_postgres,
        provide_context=True
    )

    load_dwh_task = PythonOperator(
        task_id='load_to_dwh',
        python_callable=load_to_dwh,
        provide_context=True
    )

    extract_task >> load_dwh_task