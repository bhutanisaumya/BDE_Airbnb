-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view'
    )
}}

-- Create a CTE to select data from the "room_snapshot" reference
WITH room_stg AS (
    SELECT * FROM {{ ref('room_snapshot') }}
)

--Make selections and apply transformations on the room_stg CTE
    SELECT
        listing_id,
        host_id,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        scraped_date::date,
        dbt_scd_id,
        dbt_updated_at::date,
        dbt_valid_from::date,
        CASE WHEN dbt_valid_to IS NULL THEN '9999-12-31'::date ELSE dbt_valid_to::date END AS dbt_valid_to
    FROM room_stg


