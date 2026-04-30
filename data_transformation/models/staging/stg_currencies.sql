{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_data AS (
    SELECT 
        SHORT_CODE AS currency_code,
        NAME AS currency_name  
    FROM {{ source('raw', 'currencies') }}
    ),

filtered AS (
    SELECT *
    FROM source_data
    WHERE currency_code IN ('USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'CNY', 'INR', 'NZD')
)

SELECT 
    currency_code,
    currency_name
FROM filtered



