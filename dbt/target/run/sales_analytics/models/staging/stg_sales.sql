
  create view "airflow"."test_schema"."stg_sales__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM test_schema.staging_sales
),

renamed AS (
    SELECT
        id AS sale_id,
        name AS product_name,
        amount,
        date AS sale_date,
        loaded_at
    FROM source
)

SELECT * FROM renamed
  );