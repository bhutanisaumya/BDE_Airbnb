-- Set the target schema and materialization type
{{
    config(
        target_schema='datamart',
        materialized='view'
    )
}}

-- Common Table Expression (CTE) to calculate metrics
WITH CTE_ACTIVE_LISTINGS AS (
    SELECT
        host_neighbourhood_lga_name AS host_neighbourhood_lga,
        TO_DATE(SUBSTRING(date, 1, 7), 'YYYY-MM') AS "month/year",
        COUNT(DISTINCT host_id) AS total_distinct_hosts,
        SUM(price * (30 - availability_30)) AS total_revenue
    FROM {{ ref('facts_listings') }}
    WHERE has_availability = 't'
    GROUP BY host_neighbourhood_lga, TO_DATE(SUBSTRING(date, 1, 7), 'YYYY-MM')
)

-- Query to select and calculate additional metrics
SELECT 
    host_neighbourhood_lga,
    "month/year",
    total_distinct_hosts,
    total_revenue,
    (total_revenue / NULLIF(total_distinct_hosts, 0)) AS avg_estimated_revenue_per_host
FROM CTE_ACTIVE_LISTINGS
ORDER BY host_neighbourhood_lga, "month/year"
