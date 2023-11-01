-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view'
        )
}}

-- Create a CTE to select data from the "property_snapshot" reference
WITH property_stg  AS (
    SELECT * FROM {{ ref('property_snapshot') }}
)

-- Select all records from the property_stg CTE
SELECT 
    listing_id,
    host_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    scraped_date::date,
    dbt_scd_id,
    dbt_updated_at::date,
    dbt_valid_from::date,
    CASE WHEN dbt_valid_to IS NULL THEN '9999-12-31'::date ELSE dbt_valid_to::date END AS dbt_valid_to
FROM property_stg