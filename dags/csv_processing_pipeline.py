from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import csv
import os

DATA_DIR = '/opt/airflow/data'
OUTPUT_DIR = '/opt/airflow/data/output'

default_args = {
    'owner': 'Roman',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def process_csv():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total = 0
    rows = []

    with open(f'{DATA_DIR}/sample.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += int(row['amount'])
            rows.append(row)

    with open(f'{OUTPUT_DIR}/summary.txt', 'a', encoding='utf-8') as f:
        # Пишем разделитель с временем запуска
        f.write(f"\n--- Запуск: {datetime.now()} ---\n")
        f.write(f'Всего записей: {len(rows)}\n')
        f.write(f'Общая сумма: {total}\n')
        f.write(f'Обработано: {datetime.now()}\n')

    print(f'Обработано {len(rows)} записей, сумма: {total}')


with DAG(
    'csv_processing_pipeline',
    default_args=default_args,
    description='Обработка CSV файла',
    schedule=None,
    catchup=False,
    tags=['learning', 'csv'],
) as dag:

    check_file = BashOperator(
        task_id='check_csv_exists',
        bash_command=f'test -f {DATA_DIR}/sample.csv && echo "OK" || echo "Файл не найден"',
    )

    process_data = PythonOperator(
        task_id='process_csv_file',
        python_callable=process_csv,
    )

    show_result = BashOperator(
        task_id='show_summary',
        bash_command=f'cat {OUTPUT_DIR}/summary.txt',
    )

    check_file >> process_data >> show_result