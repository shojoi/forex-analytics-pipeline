{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_data AS (
    SELECT 
        RESPONSE::VARIANT as response,
        _AIRBYTE_EXTRACTED_AT as extracted_at
    FROM {{ source('raw', 'timeseries') }}
),

flattened_dates AS (
    -- Flatten the date keys from the response object
    SELECT 
        date_key.key::STRING as rate_date_string,
        date_key.value::VARIANT as currency_rates,
        extracted_at
    FROM source_data,
    LATERAL FLATTEN(input => response) as date_key
),

flattened_currencies AS (
    -- Flatten the currency codes and rates for each date
    SELECT 
        'USD' AS base_currency,  -- Timeseries API uses USD as base (from config)
        currency.key::STRING as target_currency,
        currency.value::FLOAT as exchange_rate,
        TO_TIMESTAMP(rate_date_string, 'YYYY-MM-DD') as rate_timestamp,
        TO_DATE(rate_date_string, 'YYYY-MM-DD') as rate_date,
        'timeseries' AS source_system
    FROM flattened_dates,
    LATERAL FLATTEN(input => currency_rates) as currency
),

filtered AS (
    SELECT *
    FROM flattened_currencies
    WHERE target_currency IN (
        'USD','EUR','GBP','JPY','AUD',
        'CAD','CHF','CNY','INR','NZD'
    )
)

SELECT 
    base_currency,
    target_currency,
    exchange_rate,
    rate_timestamp,
    rate_date,
    source_system
FROM filtered
