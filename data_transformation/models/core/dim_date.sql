{{ config(
    materialized='table',
    schema='core',
    unique_key='date_key'
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

date_dimension AS (
    SELECT 
        date_day AS full_date,
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,
        YEAR(date_day) AS year,
        MONTH(date_day) AS month,
        QUARTER(date_day) AS quarter,
        DAYOFWEEK(date_day) AS day_of_week,
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM date_spine
)

SELECT * FROM date_dimension