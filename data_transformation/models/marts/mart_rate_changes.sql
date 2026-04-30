{{ config(
    materialized='table',
    schema='marts'
) }}

WITH daily_rates AS (
    SELECT * FROM {{ ref('mart_daily_rates') }}
),

with_previous AS (
    SELECT 
        rate_date,
        currency_pair,
        avg_rate AS current_rate,
        LAG(avg_rate) OVER (
            PARTITION BY currency_pair 
            ORDER BY rate_date
        ) AS previous_rate
    FROM daily_rates
),

calculated_changes AS (
    SELECT 
        rate_date,
        currency_pair,
        current_rate,
        previous_rate,
        ROUND(
            ((current_rate - previous_rate) / NULLIF(previous_rate, 0)) * 100, 
            2
        ) AS rate_change_pct,
        CASE 
            WHEN current_rate > previous_rate THEN 'UP'
            WHEN current_rate < previous_rate THEN 'DOWN'
            ELSE 'FLAT'
        END AS trend_direction
    FROM with_previous
    WHERE previous_rate IS NOT NULL
)

SELECT * FROM calculated_changes
ORDER BY rate_date DESC, currency_pair