{{ config(
    materialized='table',
    schema='core',
    unique_key='currency_key'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_currencies') }}
),

enriched AS (
    SELECT 
        currency_code,
        currency_name,
        CASE 
            WHEN currency_code IN ('EUR', 'GBP', 'CHF') THEN 'Europe'
            WHEN currency_code IN ('USD', 'CAD') THEN 'North America'
            WHEN currency_code IN ('JPY', 'CNY', 'INR', 'AUD', 'NZD') THEN 'Asia-Pacific'
            ELSE 'Other'
        END AS region,
        TRUE AS is_active,
        CURRENT_DATE() AS effective_from,
        NULL AS effective_to,
        TRUE AS is_current
    FROM source
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['currency_code', 'effective_from']) }} AS currency_key,
    currency_code,
    currency_name,
    region,
    is_active,
    effective_from,
    effective_to,
    is_current
FROM enriched