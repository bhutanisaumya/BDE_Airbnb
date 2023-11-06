-- Set the target schema and materialization type
{{
    config(
        target_schema='datamart',
        materialized='view'
    )
}}

-- Create a common table expression (CTE) to compute statistics for active listings
WITH CTE_ACTIVE_LISTINGS AS (
    SELECT
        listing_neighbourhood,
        TO_CHAR(date_trunc('month', date), 'MM/YYYY') AS "month/year",
        date_trunc('month', date) AS month,
        date_trunc('year', date) AS year,
        COUNT(DISTINCT listing_id) AS total_listings,
        COUNT(DISTINCT CASE WHEN has_availability = 't' THEN listing_id END) AS total_active_listings,
        COUNT(DISTINCT CASE WHEN has_availability = 'f' THEN listing_id END) AS total_inactive_listings,
        MIN(CASE WHEN has_availability = 't' THEN price END) AS min_price_active_listings,
        MAX(CASE WHEN has_availability = 't' THEN price END) AS max_price_active_listings,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability = 't' THEN price END) AS median_price_active_listings,
        AVG(CASE WHEN has_availability = 't' THEN price END) AS avg_price_active_listings,
        COUNT(DISTINCT host_id) AS total_distinct_hosts,
        COUNT (DISTINCT (CASE WHEN host_is_superhost = 't' THEN host_id END)) AS total_superhost_count,
        AVG(CASE WHEN has_availability = 't' THEN review_scores_rating END) AS true_avg_review_score_rating,
        SUM(CASE WHEN has_availability = 't' THEN 30 - availability_30 ELSE 0 END) AS total_number_of_stays,
        SUM(price * (30 - availability_30)) AS total_revenue
    FROM {{ ref('facts_listings') }}
    GROUP BY listing_neighbourhood, month, year
)

-- Query to calculate various statistics based on the CTE_ACTIVE_LISTINGS
SELECT
    listing_neighbourhood,
    "month/year",
    ROUND((total_active_listings::numeric / NULLIF(total_listings, 0)),2) * 100 AS active_listing_rate,
    min_price_active_listings,
    max_price_active_listings,
    median_price_active_listings,
    ROUND(avg_price_active_listings,2) AS avg_price_active_listings,
    total_distinct_hosts,
    ROUND(((total_superhost_count::numeric / NULLIF(total_distinct_hosts, 0)) * 100),2) AS superhost_rate,
    ROUND(true_avg_review_score_rating,2) AS true_avg_review_score_rating,
    ROUND((total_active_listings - LAG(total_active_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE("month/year", 'MM/YYYY'))) * 100 / NULLIF(LAG(total_active_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE("month/year", 'MM/YYYY')), 0)::numeric,2) AS percentage_change_active,
    (total_inactive_listings - LAG(total_inactive_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE("month/year", 'MM/YYYY'))) * 100 / NULLIF(LAG(total_inactive_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY TO_DATE("month/year", 'MM/YYYY')), 0)::numeric AS percentage_change_inactive,
    total_number_of_stays,
    ROUND((total_revenue / NULLIF(total_active_listings, 0)),2) AS estimated_revenue_per_active_listing
FROM CTE_ACTIVE_LISTINGS
ORDER BY listing_neighbourhood, month, year, "month/year"
