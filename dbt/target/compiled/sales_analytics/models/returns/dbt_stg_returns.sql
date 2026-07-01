WITH source AS (
    SELECT * FROM test_schema.staging_returns
),
cleaned AS (
    SELECT
        id AS return_id,
        product_name,
        amount AS return_amount,
        return_date,
        reason,
          loaded_at
    FROM source
)
SELECT * FROM cleaned