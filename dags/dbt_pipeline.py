
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import csv
import os
import psycopg2

DATA_DIR = '/opt/airflow/data'
DBT_DIR = '/opt/airflow/dbt'


def get_conn():
    return psycopg2.connect(
        host='postgres',
        port=5432,
        dbname='airflow',
        user='airflow',
        password='airflow'
    )


def load_raw_data():
    """Загружает CSV в сырую таблицу (только Extract+Load)"""
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging_sales (
                id INTEGER,
                name VARCHAR(100),
                amount INTEGER,
                date DATE,
                loaded_at TIMESTAMP DEFAULT NOW()
            );
        """)

        cursor.execute("TRUNCATE staging_sales;")

        filepath = os.path.join(DATA_DIR, 'sample.csv')
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(
                    "INSERT INTO staging_sales (id, name, amount, date) VALUES (%s, %s, %s, %s)",
                    (int(row['id']), row['name'], int(row['amount']), row['date'])
                )

        conn.commit()
        cursor.close()
        conn.close()
        print("Сырые данные загружены. Дальше работает dbt.")

    except Exception as e:
        raise AirflowException(f"Ошибка загрузки: {e}")


default_args = {
    'owner': 'Roman',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_elt_pipeline',
    default_args=default_args,
    description='ELT: Airflow загружает данные, dbt трансформирует',
    schedule=None,
    catchup=False,
    tags=['dbt', 'elt'],
) as dag:

    extract_load = PythonOperator(
        task_id='extract_and_load_csv',
        python_callable=load_raw_data,
    )

    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir profiles',
    )

    dbt_test = BashOperator(
        task_id='dbt_test_data',
        bash_command=f'cd {DBT_DIR} && dbt test --profiles-dir profiles',
    )

    extract_load >> dbt_run >> dbt_test