{{ config(
    materialized='incremental',
    schema='core',
    unique_key='rate_key',
    on_schema_change='fail'
) }}

WITH staging_rates AS (
    SELECT * FROM {{ ref('stg_rates_all_sources') }}
    {% if is_incremental() %}
    WHERE rate_timestamp > (SELECT MAX(rate_timestamp) FROM {{ this }})
    {% endif %}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_currency AS (
    SELECT * FROM {{ ref('dim_currency') }}
    WHERE is_current = TRUE
),

dim_pair AS (
    SELECT * FROM {{ ref('dim_currency_pair') }}
),

joined AS (
    SELECT 
        s.base_currency,
        s.target_currency,
        s.exchange_rate,
        s.rate_timestamp,
        s.rate_date,
        s.source_system,
        d.date_key,
        bc.currency_key AS base_currency_key,
        tc.currency_key AS target_currency_key,
        p.pair_key
    FROM staging_rates s
    INNER JOIN dim_date d 
        ON s.rate_date = d.full_date
    INNER JOIN dim_currency bc 
        ON s.base_currency = bc.currency_code
    INNER JOIN dim_currency tc 
        ON s.target_currency = tc.currency_code
    INNER JOIN dim_pair p 
        ON bc.currency_key = p.base_currency_key 
        AND tc.currency_key = p.target_currency_key
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['pair_key', 'rate_timestamp']) }} AS rate_key,
    date_key,
    pair_key,
    base_currency_key,
    target_currency_key,
    exchange_rate,
    ROUND(1.0 / NULLIF(exchange_rate, 0), 6) AS inverse_rate,
    NULL AS bid_rate,  -- Placeholder for future enhancement
    NULL AS ask_rate,  -- Placeholder for future enhancement
    rate_timestamp,
    
    source_system,
    CURRENT_TIMESTAMP() AS loaded_at
FROM joined