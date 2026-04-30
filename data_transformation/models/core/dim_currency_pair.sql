{{ config(
    materialized='table',
    schema='core',
    unique_key='pair_key'
) }}

WITH base_currencies AS (
    SELECT DISTINCT currency_code, currency_key 
    FROM {{ ref('dim_currency') }}
    WHERE is_current = TRUE
),

target_currencies AS (
    SELECT DISTINCT currency_code, currency_key 
    FROM {{ ref('dim_currency') }}
    WHERE is_current = TRUE
),

currency_pairs AS (
    SELECT 
        b.currency_key AS base_currency_key,
        t.currency_key AS target_currency_key,
        b.currency_code || '/' || t.currency_code AS pair_code
    FROM base_currencies b
    CROSS JOIN target_currencies t
    WHERE b.currency_code != t.currency_code
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['base_currency_key', 'target_currency_key']) }} AS pair_key,
    base_currency_key,
    target_currency_key,
    pair_code
FROM currency_pairs