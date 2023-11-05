-- Set the target schema and materialization type
{{ config(
    target_schema='staging',
    materialized='view'
) }}

-- Create a CTE to select data from the "room_snapshot" reference
WITH room_stg AS (
    SELECT * FROM {{ ref('room_snapshot') }}
),

-- Calculate the average price for each room_type with accommodates = 1
average_prices AS (
    SELECT
        room_type,
        AVG(price) FILTER (WHERE accommodates = 1) AS avg_price_accommodates_1
    FROM room_stg
    GROUP BY room_type
),

-- Calculate the adjusted price based on the room_type average price for accommodates = 1
calculated_prices AS (
    SELECT
        rs.listing_id,
        rs.host_id,
        rs.room_type,
        rs.accommodates,
        CASE
            WHEN rs.price < 20 THEN
                ROUND((SELECT avg_price_accommodates_1 FROM average_prices ap WHERE ap.room_type = rs.room_type) * rs.accommodates)
            WHEN rs.price > 150 THEN
                ROUND((SELECT avg_price_accommodates_1 FROM average_prices ap WHERE ap.room_type = rs.room_type) * rs.accommodates)
            ELSE
                rs.price
        END AS price,
        rs.scraped_date
        FROM room_stg rs
    WHERE (rs.price < 20 OR rs.price > 150)
)

-- Join the calculated prices with the original room_stg data
SELECT
    cp.listing_id,
    rs.host_id,
    rs.listing_neighbourhood,
    rs.property_type,
    cp.room_type,
    cp.accommodates,
    cp.price,
    rs.scraped_date::date,
    rs.dbt_scd_id,
    rs.dbt_updated_at::date,
    rs.dbt_valid_from::date,
    (CASE WHEN rs.dbt_valid_to IS NULL THEN '9999-12-31'::date ELSE rs.dbt_valid_to::date END) AS dbt_valid_to
FROM calculated_prices cp
JOIN room_stg rs ON cp.listing_id = rs.listing_id AND cp.host_id = rs.host_id
    AND rs.dbt_valid_from::timestamp = cp.scraped_date::timestamp