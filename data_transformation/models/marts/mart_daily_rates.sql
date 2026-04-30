{{ config(
    materialized='table',
    schema='marts'
) }}

WITH fact_rates AS (
    SELECT 
        f.*,
        p.pair_code
    FROM {{ ref('fact_exchange_rates') }} f
    INNER JOIN {{ ref('dim_currency_pair') }} p 
        ON f.pair_key = p.pair_key
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

daily_aggregation AS (
    SELECT 
        d.full_date AS rate_date,
        f.pair_code AS currency_pair,
        AVG(f.exchange_rate) AS avg_rate,
        MIN(f.exchange_rate) AS min_rate,
        MAX(f.exchange_rate) AS max_rate,
        STDDEV(f.exchange_rate) AS volatility
    FROM fact_rates f
    INNER JOIN date_dim d ON f.date_key = d.date_key
    GROUP BY d.full_date, f.pair_code
)

SELECT * FROM daily_aggregation
ORDER BY rate_date DESC, currency_pair