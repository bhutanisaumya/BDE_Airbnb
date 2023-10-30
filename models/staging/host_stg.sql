-- Set the target schema and materialization type
{{ config(
    target_schema='staging',
    materialized='view'
) }}

-- Create a CTE to select data from the "host_snapshot" reference
WITH source AS (
    SELECT *
    FROM {{ ref('host_snapshot') }}
),

-- CTE to clean the host data
clean_host_data AS (
    SELECT
        CASE
            WHEN host_since IS NULL OR host_since !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN '01/01/1900' ELSE host_since END as host_since,
        scraped_date,
        host_id,
        host_name,
        COALESCE(NULLIF(host_is_superhost, 'NaN'), MAX(COALESCE(NULLIF(host_is_superhost, 'NaN'), '')) OVER (PARTITION BY host_id)) AS host_is_superhost,
        COALESCE(NULLIF(host_neighbourhood, 'NaN'), MAX(COALESCE(NULLIF(host_neighbourhood, 'NaN'), '')) OVER (PARTITION BY host_id)) AS host_neighbourhood,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM source
),

-- CTE to fill missing values in host_neighbourhood
cte_fill_data AS (
    SELECT
        h.scraped_date,
        h.host_id,
        h.host_name,
        host_since,
        host_is_superhost,
        CASE WHEN h.host_neighbourhood = '' THEN (
            SELECT INITCAP(s.suburb_name) as suburb
            FROM {{ source('raw', 'lgasuburb') }} s
            JOIN {{ ref('property_snapshot') }} p ON initcap(s.lga_name) = p.listing_neighbourhood
            WHERE p.host_id = h.host_id
            LIMIT 1
        )
        ELSE h.host_neighbourhood
        END AS host_neighbourhood,
        h.dbt_scd_id,
        h.dbt_updated_at,
        h.dbt_valid_from,
        h.dbt_valid_to
    FROM clean_host_data h
)

-- Select the final result
SELECT * FROM cte_fill_data
