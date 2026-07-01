SELECT *
FROM {{ ref('dbt_stg_sales') }}
WHERE amount <= 0