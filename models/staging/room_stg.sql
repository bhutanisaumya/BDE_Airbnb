-- Set the target schema and materialization type
{{ config(
    target_schema='staging',
    materialized='view'
) }}

-- Create a CTE to select data from the "room_snapshot" reference
WITH room_stg AS (
    SELECT
        rs.listing_id,
        rs.host_id,
        rs.property_type,
        rs.room_type,
        rs.accommodates,
        CASE
            WHEN rs.price < 20 OR rs.price > 150 THEN
                ROUND(AVG(rs.price) FILTER (WHERE rs.accommodates = 1) OVER (PARTITION BY rs.room_type) * rs.accommodates)
            ELSE
                rs.price
        END AS price,
        rs.scraped_date,
        rs.listing_neighbourhood,
        rs.dbt_scd_id,
        rs.dbt_updated_at,
        rs.dbt_valid_from,
        (CASE WHEN rs.dbt_valid_to IS NULL THEN '9999-12-31'::date ELSE rs.dbt_valid_to::date END) AS dbt_valid_to
    FROM {{ ref('room_snapshot') }} rs
)

-- Select the required columns
SELECT
    r.listing_id,
    r.host_id,
    r.listing_neighbourhood,
    r.property_type,
    r.room_type,
    r.accommodates,
    r.price,
    r.scraped_date::date,
    r.dbt_scd_id,
    r.dbt_updated_at::date,
    r.dbt_valid_from::date,
    r.dbt_valid_to
FROM room_stg r
