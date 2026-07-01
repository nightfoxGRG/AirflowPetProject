

WITH new_returns AS (
    SELECT *
    FROM "airflow"."test_schema"."dbt_stg_returns"
    
    WHERE return_date > (SELECT COALESCE(MAX(return_date), '1900-01-01') FROM "airflow"."test_schema"."dbt_inc_returns_daily")
    
)
SELECT
    return_date,
    COUNT(*) AS return_count,
    SUM(return_amount) AS total_return_amount,
    STRING_AGG(reason, ', ' ORDER BY reason) AS reasons_list
FROM new_returns
GROUP BY return_date