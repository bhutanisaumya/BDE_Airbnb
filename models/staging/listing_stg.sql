-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view'
    )
}}

-- Create a CTE to select data from the raw listings table
WITH listing_stg AS (
    SELECT * FROM {{ source('raw', 'listings') }}
)

-- Select and transform data from the listing CTE
SELECT
    listing_id,
    host_id,
    scraped_date,
    has_availability,
    availability_30,
    COALESCE(NULLIF(number_of_reviews, NULL), 0) AS number_of_reviews,
    CASE
        WHEN review_scores_rating = 'NaN' THEN 0
        ELSE COALESCE(review_scores_rating::numeric, 0)
    END AS review_scores_rating
FROM listing_stg
