{{ config(
    schema='warehouse',
    materialized='table'
)}}

WITH check_dimensions AS (
    SELECT
        listing_id,
        host_id,
        scraped_date, 
        has_availability,
        availability_30,
        number_of_reviews,
        review_scores_rating
    FROM {{ ref('listing_stg') }}
),

cte_interim AS (
    SELECT
        a.listing_id,
        a.scraped_date,
        b.listing_neighbourhood,
        b.property_type,
        a.host_id,
        c.host_name,
        c.host_since,
        c.host_is_superhost,
        c.host_neighbourhood,
        d.room_type,
        d.price,
        d.accommodates,
        a.has_availability,
        a.availability_30,
        a.number_of_reviews,
        a.review_scores_rating
    FROM check_dimensions a
    LEFT JOIN staging.property_stg b ON a.listing_id = b.listing_id 
        AND a.scraped_date::timestamp >= b.dbt_valid_from::timestamp
        AND a.scraped_date::timestamp < COALESCE(b.dbt_valid_to::timestamp, '9999-12-31'::timestamp)
    LEFT JOIN staging.host_stg c ON a.host_id = c.host_id 
        AND a.scraped_date::timestamp >= c.dbt_valid_from::timestamp
        AND a.scraped_date::timestamp < COALESCE(c.dbt_valid_to::timestamp, '9999-12-31'::timestamp)
    LEFT JOIN staging.room_stg d ON (a.listing_id = d.listing_id AND a.scraped_date = d.scraped_date)
        AND a.scraped_date::timestamp >= d.dbt_valid_from::timestamp
        AND a.scraped_date::timestamp < COALESCE(d.dbt_valid_to::timestamp, '9999-12-31'::timestamp)
),

cte_host_lga_name AS (
    SELECT
        a.listing_id,
        a.scraped_date,
        a.listing_neighbourhood,
        a.host_id,
        a.host_name,
        a.host_since,
        a.host_is_superhost,
        a.host_neighbourhood,
        a.property_type,
        a.room_type,
        a.price,
        a.accommodates,
        a.has_availability,
        a.availability_30,
        a.number_of_reviews,
        a.review_scores_rating,
        b.suburb_name,
        b.lga_name as host_neighbourhood_lga_name
    FROM cte_interim a
    LEFT JOIN staging.lgasuburb_stg b ON a.host_neighbourhood = b.suburb_name
),

cte_host_lga_code AS (
    SELECT
        a.listing_id,
        a.scraped_date,
        a.listing_neighbourhood,
        a.host_id,
        a.host_name,
        a.host_since,
        a.host_is_superhost,
        a.host_neighbourhood,
        a.property_type,
        a.room_type,
        a.price,
        a.accommodates,
        a.has_availability,
        a.availability_30,
        a.number_of_reviews,
        a.review_scores_rating,
        a.host_neighbourhood_lga_name,
        b.lga_name,
        b.lga_code as host_neighbourhood_lga_code
    FROM cte_host_lga_name a
    LEFT JOIN staging.lgacode_stg b ON a.host_neighbourhood_lga_name = b.lga_name
)

SELECT
    listing_id,
    scraped_date as date,
    listing_neighbourhood,
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood_lga_code,
    host_neighbourhood_lga_name,
    host_neighbourhood,
    property_type,
    room_type,
    price,
    accommodates,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating
FROM cte_host_lga_code
