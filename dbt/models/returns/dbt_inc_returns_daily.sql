{{ config(
    materialized='incremental',
    unique_key='return_date'
) }}

WITH new_returns AS (
    SELECT *
    FROM {{ ref('dbt_stg_returns') }}
    {% if is_incremental() %}
    WHERE return_date > (SELECT COALESCE(MAX(return_date), '1900-01-01') FROM {{ this }})
    {% endif %}
)
SELECT
    return_date,
    COUNT(*) AS return_count,
    SUM(return_amount) AS total_return_amount,
    STRING_AGG(reason, ', ' ORDER BY reason) AS reasons_list
FROM new_returns
GROUP BY return_date