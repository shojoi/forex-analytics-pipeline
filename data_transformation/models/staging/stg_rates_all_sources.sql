{{ config(
    materialized='view',
    schema='staging'
) }}

WITH all_rates AS (

    -- Historical and Latest rates (from stg_rates_flattened)
    SELECT
        base_currency,
        target_currency,
        exchange_rate,
        rate_timestamp,
        rate_date,
        source_system
    FROM {{ ref('stg_rates_flattened') }}

    UNION ALL

    -- Timeseries rates (from stg_timeseries_rates_flattened)
    SELECT
        base_currency,
        target_currency,
        exchange_rate,
        rate_timestamp,
        rate_date,
        source_system
    FROM {{ ref('stg_timeseries_rates_flattened') }}

),

deduplicated AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY base_currency, target_currency, rate_date
            ORDER BY 
                rate_timestamp DESC,
                CASE source_system
                    WHEN 'latest' THEN 1
                    WHEN 'timeseries' THEN 2
                    WHEN 'historical' THEN 3
                END
        ) AS row_number

    FROM all_rates

)

SELECT
    base_currency,
    target_currency,
    exchange_rate,
    rate_timestamp,
    rate_date,
    source_system

FROM deduplicated
WHERE row_number = 1
