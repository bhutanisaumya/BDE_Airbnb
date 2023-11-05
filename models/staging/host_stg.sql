-- Set the target schema and materialization type
{{ config(
    target_schema='staging',
    materialized='view'
) 
}}

-- Create a CTE to select data from the "host_snapshot" reference
WITH source AS (
    SELECT
        host_id,
        COALESCE(NULLIF(host_name, 'NaN'), MAX(NULLIF(host_name, 'NaN')) OVER (PARTITION BY host_id), '') AS host_name,
        COALESCE(
            CASE
                WHEN host_since IS NULL OR host_since !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN NULL
                ELSE to_date(host_since, 'DD/MM/YYYY')
            END,
            MAX(
                CASE
                    WHEN host_since IS NULL OR host_since !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN NULL
                    ELSE to_date(host_since, 'DD/MM/YYYY')
                END
            ) OVER (PARTITION BY host_id)
        ) AS host_since,
        COALESCE(NULLIF(host_is_superhost, 'NaN'), MAX(COALESCE(NULLIF(host_is_superhost, 'NaN'), '')) OVER (PARTITION BY host_id), '') AS host_is_superhost,
        COALESCE(NULLIF(host_neighbourhood, 'NaN'), MAX(COALESCE(NULLIF(host_neighbourhood, 'NaN'), '')) OVER (PARTITION BY host_id), '') AS host_neighbourhood,
        scraped_date::date,
        dbt_scd_id,
        dbt_updated_at::date,
        dbt_valid_from::date,
        CASE WHEN dbt_valid_to IS NULL THEN '9999-12-31'::date ELSE dbt_valid_to::date END AS dbt_valid_to
    FROM {{ ref('host_snapshot') }}
),
     
-- CTE to fill missing values in host_neighbourhood
cte_fill_data AS (
    SELECT
        h.host_id,
        h.host_name,
        h.host_since,
        h.host_is_superhost,
        CASE WHEN h.host_neighbourhood = '' THEN (
            SELECT INITCAP(s.suburb_name) as suburb
            FROM {{ source('raw', 'lgasuburb') }} s
            JOIN {{ ref('property_snapshot') }} p ON initcap(s.lga_name) = p.listing_neighbourhood
            WHERE p.host_id = h.host_id
            LIMIT 1
        )
        ELSE h.host_neighbourhood END,
        h.scraped_date,
        h.dbt_scd_id,
        h.dbt_updated_at,
        h.dbt_valid_from,
        h.dbt_valid_to
    FROM source h
)

-- Select the final result
SELECT * FROM cte_fill_data




