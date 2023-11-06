-- Set the target schema and materialization type
{{
    config(
        target_schema='datamart',
        materialized='view'
    )
}}

-- Create a common table expression (CTE) to compute metrics related to hosts and their neighborhoods
WITH CTE_HOST_METRICS AS (
    SELECT
        host_neighbourhood_lga_name AS host_neighbourhood_lga,
        TO_CHAR(date_trunc('month', date), 'MM/YYYY') AS "month/year",
        date_trunc('month', date) AS month,
        date_trunc('year', date) AS year,
        COUNT(DISTINCT host_id) AS total_distinct_hosts,
        SUM(CASE WHEN host_neighbourhood_lga_name IS NOT NULL THEN price * (30 - availability_30) ELSE 0 END) AS total_revenue
    FROM {{ ref('facts_listings') }}
    WHERE host_neighbourhood_lga_name IS NOT NULL
    GROUP BY host_neighbourhood_lga,  month, year
)

-- Query to extract and calculate host metrics
SELECT 
    host_neighbourhood_lga,
    "month/year",
    total_distinct_hosts,
    ROUND(total_revenue,2) AS total_revenue,
    ROUND((total_revenue / NULLIF(total_distinct_hosts, 0)),2) AS avg_estimated_revenue_per_host
FROM CTE_HOST_METRICS
ORDER BY host_neighbourhood_lga, month, year
