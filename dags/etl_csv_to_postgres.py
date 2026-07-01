from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import csv
import os
import psycopg2

DATA_DIR = '/opt/airflow/data'


def get_conn():
    """Создаёт подключение к Postgres"""
    return psycopg2.connect(
        host='postgres',
        port=5432,
        dbname='airflow',
        user='airflow',
        password='airflow'
    )


def load_csv_to_postgres():
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_schema.staging_sales (
                id INTEGER,
                name VARCHAR(100),
                amount INTEGER,
                date DATE,
                loaded_at TIMESTAMP DEFAULT NOW()
            );
        """)

        cursor.execute("TRUNCATE test_schema.staging_sales;")

        filepath = os.path.join(DATA_DIR, 'sample.csv')
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(
                    "INSERT INTO test_schema.staging_sales (id, name, amount, date) VALUES (%s, %s, %s, %s)",
                    (int(row['id']), row['name'], int(row['amount']), row['date'])
                )

        conn.commit()
        cursor.close()
        conn.close()
        print("Данные загружены в staging_sales")

    except Exception as e:
        raise AirflowException(f"Ошибка загрузки CSV: {e}")


def create_sales_mart():
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS test_schema.mart_sales_summary;")
        cursor.execute("""
            CREATE TABLE test_schema.mart_sales_summary AS
            SELECT
                DATE_TRUNC('month', date) AS month,
                SUM(amount) AS total_amount,
                COUNT(*) AS transaction_count
            FROM test_schema.staging_sales
            GROUP BY DATE_TRUNC('month', date);
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Витрина mart_sales_summary создана")

    except Exception as e:
        raise AirflowException(f"Ошибка создания витрины: {e}")


def check_mart_data():
    try:
        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM test_schema.mart_sales_summary;")
        rows = cursor.fetchall()
        for row in rows:
            print(f"Месяц: {row[0]}, Сумма: {row[1]}, Кол-во: {row[2]}")

        cursor.close()
        conn.close()

    except Exception as e:
        raise AirflowException(f"Ошибка чтения витрины: {e}")


default_args = {
    'owner': 'Roman',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_csv_to_postgres',
    default_args=default_args,
    description='ETL: загрузка CSV в PostgreSQL и построение витрины',
    schedule=None,
    catchup=False,
    tags=['etl', 'postgres'],
) as dag:

    load_staging = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=load_csv_to_postgres,
    )

    create_mart = PythonOperator(
        task_id='create_sales_mart',
        python_callable=create_sales_mart,
    )

    check_mart = PythonOperator(
        task_id='check_mart_data',
        python_callable=check_mart_data,
    )

    load_staging >> create_mart >> check_mart