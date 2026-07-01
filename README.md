# airflowPetProject

Учебный (pet) проект по data engineering: локальный стенд для отработки ETL/ELT-пайплайнов на **Apache Airflow + dbt + PostgreSQL**, поднимаемый одной командой через Docker Compose. Рядом развёрнут **Apache NiFi** для экспериментов с потоковой обработкой данных.

Проект демонстрирует несколько подходов к загрузке и трансформации данных: от «ручного» ETL на psycopg2 до ELT с трансформациями в dbt, включая чанковую загрузку и инкрементальные модели.

---

## Стек технологий

- **Apache Airflow 3.2.2** — оркестрация пайплайнов (CeleryExecutor)
- **PostgreSQL 18.4** — метабаза Airflow + хранилище данных (схема `test_schema`)
- **Redis 8.x** — брокер сообщений для Celery
- **dbt-postgres 1.9.1** — трансформации данных (SQL-модели)
- **Apache NiFi 2.6.0** — потоковая обработка данных

Сервисы Airflow: `apiserver`, `scheduler`, `dag-processor`, `worker`, `triggerer`, `init`.
Опциональные профили Compose: `flower` (мониторинг Celery, порт 5555) и `debug` (`airflow-cli`).

---

## Структура проекта

```
airflowPetProject/
├── dags/                # DAG-и Airflow
│   ├── csv_processing_pipeline.py   # обучающий пайплайн обработки CSV
│   ├── etl_csv_to_postgres.py       # ETL на psycopg2
│   ├── dbt_pipeline.py              # ELT (Airflow + dbt run/test)
│   └── returns_elt_pipeline.py      # ELT возвратов (чанки + инкремент)
├── dbt/                 # dbt-проект sales_analytics
│   ├── models/          # staging / marts / returns
│   ├── tests/           # singular-тесты
│   ├── profiles/        # profiles.yml (подключение к Postgres)
│   └── dbt_project.yml
├── data/                # тестовые CSV (sample, returns, products)
├── config/              # кастомный airflow.cfg
├── docker/              # docker-compose.yaml + .env
├── plugins/             # плагины Airflow (пусто)
└── logs/                # логи выполнения DAG-ов
```

---

## Быстрый старт

Требуется Docker (≥ 4 GB RAM, ≥ 2 CPU).

```bash
cd docker
docker compose -f docker-compose.yaml -p airflow_pet_project up -d
```

Доступы после запуска:

- **Airflow UI** → http://localhost:8001 — `airflow` / `airflow`
- **NiFi UI** → https://localhost:8443 — `admin` / `adminadminadmin`
- **PostgreSQL** → `localhost:5432`, БД `airflow` — `airflow` / `airflow`

dbt-адаптер (`dbt-postgres==1.9.1`) устанавливается в контейнеры автоматически при старте через `_PIP_ADDITIONAL_REQUIREMENTS`.

Локальные каталоги проекта монтируются в `/opt/airflow/*` (dags, logs, config, plugins, data, dbt).

---

## Пайплайны (DAG-и)

Все DAG-и запускаются вручную (`schedule=None`, `catchup=False`), `retries=1`.

### `csv_processing_pipeline` · теги: `learning`, `csv`

Обучающий пайплайн. Проверяет наличие `sample.csv`, считает количество записей и сумму `amount`, дописывает результат в `data/output/summary.txt`.

```
check_csv_exists → process_csv_file → show_summary
```

### `etl_csv_to_postgres` · теги: `etl`, `postgres`

Классический **ETL** на чистом psycopg2. Грузит `sample.csv` в `test_schema.staging_sales`, строит витрину `mart_sales_summary` с агрегатом продаж по месяцам, выводит результат в лог.

```
load_csv_to_staging → create_sales_mart → check_mart_data
```

### `dbt_elt_pipeline` · теги: `dbt`, `elt`

**ELT**-пайплайн: Airflow отвечает только за Extract + Load (сырьё → `staging_sales`), трансформации полностью делегированы dbt.

```
extract_and_load_csv → dbt_run_models → dbt_test_data
```

### `returns_elt_pipeline` · теги: `dbt`, `elt`, `returns`, `incremental`

**ELT возвратов** с чанковой загрузкой: читает `returns.csv` пачками по 1000 строк, вставляет одним SQL-запросом через `unnest` в `test_schema.staging_returns`, затем dbt строит инкрементальную витрину по дням.

```
extract_and_load_returns → dbt_run_returns_models
```

---

## dbt-проект `sales_analytics`

Профиль подключается к Postgres, целевая схема — `test_schema` (`dbt/profiles/profiles.yml`).

### Модели

**staging** (материализация: `view`)

- **`dbt_stg_sales`** — очистка и переименование полей `staging_sales` (`id → sale_id`, `name → product_name`, `date → sale_date`)
- **`dbt_stg_returns`** — очистка и переименование полей `staging_returns` (`id → return_id`, `amount → return_amount`)

**marts** (материализация: `table`)

- **`dbt_mart_sales_summary`** — помесячные агрегаты продаж: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX` по полю `amount`

**returns** (материализация: `incremental`)

- **`dbt_inc_returns_daily`** — витрина возвратов по дням: `COUNT`, `SUM(return_amount)`, `STRING_AGG(reason)`. Инкремент по `return_date` — при повторном запуске добавляются только новые даты.

### Тесты

- `tests/assert_amount_positive.sql` — singular-тест: проверяет, что в `dbt_stg_sales` нет продаж с `amount <= 0`.

### Генерация документации

```bash
docker compose -f docker-compose.yaml -p airflow_pet_project \
  exec airflow-worker bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir profiles"
```

---

## Тестовые данные (`data/`)

- **`sample.csv`** — продажи: `id, name, amount, date`
- **`returns.csv`** — возвраты: `id, product_name, amount, return_date, reason`
- **`products.csv`** — справочник товаров: `product_id, product_name, category, price, valid_from`

---

## Заметки

- Это **учебный проект**, конфигурация рассчитана на локальную разработку и **не предназначена для production**.
- Пароли и ключи (`airflow/airflow`, NiFi, Fernet) — дефолтные/демонстрационные, менять при любом реальном использовании.
- DAG-и стартуют в статусе paused (`DAGS_ARE_PAUSED_AT_CREATION=true`); включены примеры Airflow (`LOAD_EXAMPLES=true`).
