
      
        
        
        delete from "airflow"."test_schema"."dbt_inc_returns_daily" as DBT_INTERNAL_DEST
        where (return_date) in (
            select distinct return_date
            from "dbt_inc_returns_daily__dbt_tmp140112982658" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airflow"."test_schema"."dbt_inc_returns_daily" ("return_date", "return_count", "total_return_amount", "reasons_list")
    (
        select "return_date", "return_count", "total_return_amount", "reasons_list"
        from "dbt_inc_returns_daily__dbt_tmp140112982658"
    )
  