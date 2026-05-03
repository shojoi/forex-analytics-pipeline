{{ config(
    materialized='view',
    schema='staging'
) }}

WITH combined AS (

    SELECT
        RESPONSE:base::string AS base_currency,
        RESPONSE:date::timestamp AS rate_timestamp,
        RESPONSE:rates AS rates,
        'historical' AS source_system
    FROM {{ source('raw', 'historical_rates') }}

    UNION ALL

    SELECT
        RESPONSE:base::string AS base_currency,
        RESPONSE:date::timestamp AS rate_timestamp,
        RESPONSE:rates AS rates,
        'latest' AS source_system
    FROM {{ source('raw', 'rates') }}

),

flattened AS (

    SELECT
        base_currency,
        f.key::string AS target_currency,
        f.value::float AS exchange_rate,
        rate_timestamp,
        CAST(rate_timestamp AS DATE) AS rate_date,
        source_system

    FROM combined,
    LATERAL FLATTEN(input => rates) f

),

filtered AS (

    SELECT *
    FROM flattened
    WHERE target_currency IN (
        'USD','EUR','GBP','JPY','AUD',
        'CAD','CHF','CNY','INR','NZD'
    )

),

deduplicated AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY base_currency, target_currency, rate_timestamp
            ORDER BY 
                CASE source_system
                    WHEN 'latest' THEN 1
                    WHEN 'timeseries' THEN 2
                    WHEN 'historical' THEN 3
                END
        ) AS row_number

    FROM filtered

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