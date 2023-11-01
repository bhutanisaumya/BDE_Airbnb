-- Set the target schema and materialization type
{{
    config(
        target_schema='datamart',
        materialized='view'
    )
}}

WITH CTE_ACTIVE_LISTINGS AS (
    SELECT
        listing_neighbourhood,
        TO_DATE(SUBSTRING(date, 1, 7), 'YYYY-MM') AS "month/year",
        COUNT(DISTINCT listing_id) AS total_listings,
        COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) AS total_active_listings,
        MIN(CASE WHEN has_availability = 't' THEN price END) AS min_price_active_listings,
        MAX(CASE WHEN has_availability = 't' THEN price END) AS max_price_active_listings,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability = 't' THEN price END) AS median_price_active_listings,
        AVG(CASE WHEN has_availability = 't' THEN price END) AS avg_price_active_listings,
        COUNT(DISTINCT host_id) AS total_distinct_hosts,
        COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id END) AS total_superhost_count,
        AVG(CASE WHEN has_availability = 't' THEN review_scores_rating END) AS true_avg_review_score_rating,
        SUM(CASE WHEN has_availability = 't' THEN 30 - availability_30 ELSE 0 END) AS total_number_of_stays,
        SUM(price * (30 - availability_30)) AS total_revenue
    FROM {{ ref('facts_listings') }}
    GROUP BY listing_neighbourhood, TO_DATE(SUBSTRING(date, 1, 7), 'YYYY-MM')
)

SELECT
    listing_neighbourhood,
    "month/year",
    total_listings,
    total_active_listings,
    min_price_active_listings,
    max_price_active_listings,
    median_price_active_listings,
    avg_price_active_listings,
    total_distinct_hosts,
    (total_superhost_count / NULLIF(total_distinct_hosts, 0)) * 100 AS superhost_rate,
    true_avg_review_score_rating,
    (total_active_listings - LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY "month/year")) * 100 / NULLIF(LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY "month/year"), 0) AS percentage_change_active,
    (total_listings - total_active_listings - (LAG(total_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY "month/year") - LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY "month/year"))) * 100 / NULLIF((LAG(total_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY "month/year") - LAG(total_active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY "month/year")), 0) AS percentage_change_inactive,
    total_number_of_stays,
    (total_revenue / NULLIF(total_listings, 0)) AS avg_rev
FROM CTE_ACTIVE_LISTINGS
ORDER BY listing_neighbourhood, "month/year"
