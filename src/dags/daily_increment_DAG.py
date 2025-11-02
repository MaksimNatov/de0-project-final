from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import pandas as pd
import os
import tempfile

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
    airflow_root = os.path.dirname(os.path.dirname(__file__))
    sql_path = os.path.join(airflow_root, 'sql', filename)
    return sql_path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def fast_bulk_load_to_vertica(hook, table_name, data, columns):
    """Быстрая загрузка данных в Vertica через CSV файл"""
    if not data:
        logger.info(f"No data to load for {table_name}")
        return
    
    # Создаем временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        temp_path = temp_file.name
        try:
            # Записываем данные в CSV
            df = pd.DataFrame(data, columns=columns)
            df.to_csv(temp_path, index=False, header=False)
            
            # Формируем имена колонок
            columns_str = ', '.join(columns)
            
            # Выполняем COPY команду
            copy_sql = f"""
                COPY {table_name} ({columns_str}) 
                FROM LOCAL '{temp_path}' 
                DELIMITER ',' 
                NULL ''
                ABORT ON ERROR
                ENFORCELENGTH
            """
            
            logger.info(f"Loading {len(data)} rows into {table_name}...")
            hook.run(copy_sql)
            logger.info(f"Successfully loaded {len(data)} rows into {table_name}")
            
        finally:
            # Удаляем временный файл
            if os.path.exists(temp_path):
                os.unlink(temp_path)

def extract_from_postgres(**context):
    """Оптимизированное извлечение данных из PostgreSQL"""
    yesterday = context['yesterday_ds']
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    vertica_hook = VerticaHook(vertica_conn_id='vertica_conn')
    
    # ЗАГРУЗКА ТРАНЗАКЦИЙ
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
    
    logger.info(f"Loading transactions data for {yesterday}...")
    
    # Используем pandas для эффективного чтения данных
    transactions_df = postgres_hook.get_pandas_df(transactions_query)
    
    if not transactions_df.empty:
        # Конвертируем в список значений
        transactions_data = [tuple(x) for x in transactions_df.to_numpy()]
        
        columns = [
            'operation_id', 'account_number_from', 'account_number_to', 
            'currency_code', 'country', 'status', 'transaction_type', 
            'amount', 'transaction_dt'
        ]
        
        fast_bulk_load_to_vertica(
            vertica_hook, 
            'STV202506164__STAGING.transactions', 
            transactions_data, 
            columns
        )
    else:
        logger.info("No transactions data to load")
    
    # ЗАГРУЗКА КУРСОВ ВАЛЮТ
    currencies_query = f"""
        SELECT 
            date_update,
            currency_code,
            currency_code_with,
            currency_with_div
        FROM public.currencies 
        WHERE date_update::date = '{yesterday}'
    """
    
    logger.info(f"Loading currencies data for {yesterday}...")
    currencies_df = postgres_hook.get_pandas_df(currencies_query)
    
    if not currencies_df.empty:
        currencies_data = [tuple(x) for x in currencies_df.to_numpy()]
        
        columns = ['date_update', 'currency_code', 'currency_code_with', 'currency_with_div']
        
        fast_bulk_load_to_vertica(
            vertica_hook,
            'STV202506164__STAGING.currencies',
            currencies_data,
            columns
        )
    else:
        logger.warning(f"No currencies data found for date: {yesterday}")
    
    logger.info(f"Data extraction completed for date: {yesterday}")

def load_to_dwh(**context):
    """Загрузка данных в витрину DWH"""
    vertica_hook = VerticaHook(vertica_conn_id='vertica_conn')
    
    merge_sql_path = get_sql_file_path('increment.sql')
    merge_sql = read_sql_file(merge_sql_path, **context)
    
    logger.info("Executing MERGE query...")
    vertica_hook.run(merge_sql)
    logger.info(f"Data successfully loaded to DWH for date: {context['yesterday_ds']}")

with DAG(
    'daily_loading_metrics_dag',
    default_args=default_args,
    description='DAG for daily loading metrics from PostgreSQL to Vertica DWH',
    schedule_interval='0 1 * * *',
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