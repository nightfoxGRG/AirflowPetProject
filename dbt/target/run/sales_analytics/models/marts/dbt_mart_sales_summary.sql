
  
    

  create  table "airflow"."test_schema"."dbt_mart_sales_summary__dbt_tmp"
  
  
    as
  
  (
    WITH sales AS (
    SELECT * FROM "airflow"."test_schema"."dbt_stg_sales"
),

aggregated AS (
    SELECT
        DATE_TRUNC('month', sale_date) AS month,
        SUM(amount) AS total_amount,
        COUNT(*) AS transaction_count,
        AVG(amount) AS avg_amount,
        MIN(amount) AS min_amount,
        MAX(amount) AS max_amount
    FROM sales
    GROUP BY DATE_TRUNC('month', sale_date)
)

SELECT * FROM aggregated
  );
  