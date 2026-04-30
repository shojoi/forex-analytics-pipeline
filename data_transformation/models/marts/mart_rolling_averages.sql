{{ config(
    materialized='table',
    schema='marts'
) }}

WITH daily_rates AS (
    SELECT * FROM {{ ref('mart_daily_rates') }}
),

with_moving_averages AS (
    SELECT 
        rate_date,
        currency_pair,
        avg_rate AS current_rate,
        AVG(avg_rate) OVER (
            PARTITION BY currency_pair 
            ORDER BY rate_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7day,
        AVG(avg_rate) OVER (
            PARTITION BY currency_pair 
            ORDER BY rate_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ma_30day
    FROM daily_rates
)

SELECT 
    rate_date,
    currency_pair,
    ROUND(current_rate, 6) AS current_rate,
    ROUND(ma_7day, 6) AS ma_7day,
    ROUND(ma_30day, 6) AS ma_30day
FROM with_moving_averages
ORDER BY rate_date DESC, currency_pair