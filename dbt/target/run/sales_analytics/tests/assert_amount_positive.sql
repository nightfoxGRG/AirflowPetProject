
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT *
FROM "airflow"."test_schema"."dbt_stg_sales"
WHERE amount <= 0
  
  
      
    ) dbt_internal_test