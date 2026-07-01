from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import csv
import os
import psycopg2

DATA_DIR = '/opt/airflow/data'
DBT_DIR = '/opt/airflow/dbt'
CHUNK_SIZE = 1000


def get_conn():
    return psycopg2.connect(
        host='postgres', port=5432,
        dbname='airflow', user='airflow', password='airflow'
    )


def _insert_chunk(cursor, rows):
    """Вставляет список кортежей одним запросом через unnest."""
    ids = [r[0] for r in rows]
    names = [r[1] for r in rows]
    amounts = [r[2] for r in rows]
    dates = [r[3] for r in rows]
    reasons = [r[4] for r in rows]

    cursor.execute("""
        INSERT INTO test_schema.staging_returns (id, product_name, amount, return_date, reason)
        SELECT * FROM unnest(
            %s::int[],
            %s::varchar[],
            %s::int[],
            %s::date[],
            %s::varchar[]
        )
    """, (ids, names, amounts, dates, reasons))


def load_returns_chunked():
    """Читает returns.csv чанками и заливает в staging_returns через unnest."""
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Создать таблицу, если нет
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_schema.staging_returns (
                id INTEGER,
                product_name VARCHAR(100),
                amount INTEGER,
                return_date DATE,
                reason VARCHAR(255),
                loaded_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Очистить перед загрузкой (идемпотентность)
        cursor.execute("TRUNCATE test_schema.staging_returns;")

        filepath = os.path.join(DATA_DIR, 'returns.csv')

        total_rows = 0
        chunk_num = 0

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            chunk = []

            for row in reader:
                chunk.append((
                    int(row['id']),
                    row['product_name'],
                    int(row['amount']),
                    row['return_date'],
                    row['reason']
                ))

                if len(chunk) >= CHUNK_SIZE:
                    _insert_chunk(cursor, chunk)
                    total_rows += len(chunk)
                    chunk_num += 1
                    print(f"Пакет {chunk_num}: {len(chunk)} записей")
                    chunk = []

            # Последний неполный чанк
            if chunk:
                _insert_chunk(cursor, chunk)
                total_rows += len(chunk)
                chunk_num += 1
                print(f"Пакет {chunk_num}: {len(chunk)} записей (последний)")

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Готово. Всего загружено {total_rows} записей, пакетов: {chunk_num}")

    except Exception as e:
        raise AirflowException(f"Ошибка загрузки возвратов: {e}")


default_args = {
    'owner': 'Roman',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'returns_elt_pipeline',
    default_args=default_args,
    description='ELT возвратов: чанковая загрузка CSV → dbt инкрементальная витрина',
    schedule=None,
    catchup=False,
    tags=['dbt', 'elt', 'returns', 'incremental'],
) as dag:

    extract_load = PythonOperator(
        task_id='extract_and_load_returns',
        python_callable=load_returns_chunked,
    )

    dbt_run = BashOperator(
        task_id='dbt_run_returns_models',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir profiles --select returns',
    )

    extract_load >> dbt_run

    # dbt_test = BashOperator(
    #     task_id='dbt_test_returns',
    #     bash_command=f'cd {DBT_DIR} && dbt test --profiles-dir profiles --select returns',
    # )
    #
    # extract_load >> dbt_run >> dbt_test