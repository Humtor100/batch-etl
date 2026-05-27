from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'Im,
    'depends_on_past': False,
    'email_his_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_raw_data_quality():
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')

    query = """
        select count(*) from raw_transactions
        where amount <=0 or amount is null or client_id is null;
    """

    result = hook.get_first(query)
    bad_rows_count = result[0]
    if bad_rows_count > 0:
        raise AirflowException("Есть строки с пустыми или отрицательными значениями")
    else:
        print("Проверка успешна")

with DAG(
    'client_activity_daily_aggregation',
    default_args=default_args,
    description='Ежедневный расчет витрины активности клиентов',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'aggregations'],
) as dag:
    check_dq_task = PythonOperator(
        task_id='check_raw_data_quality',
        python_callable=check_raw_data_quality,
    )

    aggregate_data_task = PostgresOperator(
        task_id='aggregate_transactions_to_mart',
        postgres_conn_id='my_postgres_conn',
        sql="""
            INSERT INTO dm_client_activity (client_id, activity_date, total_amount, transaction_count)
            SELECT 
                client_id,
                transaction_date::date AS activity_date,
                SUM(amount) AS total_amount,
                COUNT(transaction_id) AS transaction_count
            FROM raw_transactions
            GROUP BY client_id, transaction_date::date
            ON CONFLICT (client_id, activity_date) 
            DO UPDATE SET 
                total_amount = EXCLUDED.total_amount,
                transaction_count = EXCLUDED.transaction_count;
        """
    )

    check_dq_task >> aggregate_data_task