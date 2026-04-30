{{ config(
    materialized='table',
    schema='marts'
) }}

WITH fact_rates AS (
    SELECT 
        f.*,
        p.pair_code,
        d.full_date,
        DATE_TRUNC('month', d.full_date) AS month
    FROM {{ ref('fact_exchange_rates') }} f
    INNER JOIN {{ ref('dim_currency_pair') }} p 
        ON f.pair_key = p.pair_key
    INNER JOIN {{ ref('dim_date') }} d 
        ON f.date_key = d.date_key
),

monthly_metrics AS (
    SELECT 
        month,
        pair_code AS currency_pair,
        AVG(exchange_rate) AS month_avg_rate,
        FIRST_VALUE(exchange_rate) OVER (
            PARTITION BY month, pair_code 
            ORDER BY rate_timestamp
        ) AS month_open_rate,
        LAST_VALUE(exchange_rate) OVER (
            PARTITION BY month, pair_code 
            ORDER BY rate_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS month_close_rate
    FROM fact_rates
    GROUP BY month, pair_code, exchange_rate, rate_timestamp
),

deduplicated AS (
    SELECT DISTINCT
        month,
        currency_pair,
        month_avg_rate,
        month_open_rate,
        month_close_rate
    FROM monthly_metrics
),

with_changes AS (
    SELECT 
        month,
        currency_pair,
        ROUND(month_avg_rate, 6) AS month_avg_rate,
        ROUND(month_open_rate, 6) AS month_open_rate,
        ROUND(month_close_rate, 6) AS month_close_rate,
        ROUND(
            ((month_close_rate - month_open_rate) / NULLIF(month_open_rate, 0)) * 100, 
            2
        ) AS month_change_pct
    FROM deduplicated
)

SELECT * FROM with_changes
ORDER BY month DESC, currency_pair