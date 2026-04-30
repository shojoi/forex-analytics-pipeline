{{ config(
    materialized='table',
    schema='marts'
) }}

WITH daily_rates AS (
    SELECT * FROM {{ ref('mart_daily_rates') }}
),

-- Split currency pairs to get individual currency performance
base_currency_rates AS (
    SELECT 
        rate_date,
        SPLIT_PART(currency_pair, '/', 1) AS currency_code,
        avg_rate,
        SPLIT_PART(currency_pair, '/', 2) AS vs_currency
    FROM daily_rates
),

-- Calculate average rate for each currency against major currencies
currency_strength AS (
    SELECT 
        rate_date,
        currency_code,
        AVG(CASE WHEN vs_currency = 'USD' THEN avg_rate END) AS avg_rate_vs_usd,
        AVG(CASE WHEN vs_currency = 'EUR' THEN avg_rate END) AS avg_rate_vs_eur,
        AVG(CASE WHEN vs_currency = 'GBP' THEN avg_rate END) AS avg_rate_vs_gbp,
        AVG(avg_rate) AS strength_index,
        COUNT(DISTINCT vs_currency) AS num_pairs_calculated
    FROM base_currency_rates
    WHERE vs_currency IN ('USD', 'EUR', 'GBP')
    GROUP BY rate_date, currency_code
),

with_ranking AS (
    SELECT 
        rate_date,
        currency_code,
        ROUND(strength_index, 6) AS strength_index,
        ROUND(avg_rate_vs_usd, 6) AS avg_rate_vs_usd,
        ROUND(avg_rate_vs_eur, 6) AS avg_rate_vs_eur,
        ROUND(avg_rate_vs_gbp, 6) AS avg_rate_vs_gbp,
        num_pairs_calculated,
        RANK() OVER (
            PARTITION BY rate_date 
            ORDER BY strength_index DESC
        ) AS strength_rank
    FROM currency_strength
)

SELECT * FROM with_ranking
ORDER BY rate_date DESC, strength_rank