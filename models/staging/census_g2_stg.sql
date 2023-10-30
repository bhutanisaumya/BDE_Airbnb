-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view',
        )
}}

-- Create a CTE for the census data from the "census_g2" source
WITH census_g2_stg AS (
    SELECT * FROM {{ source('raw', 'census_g2') }}
)

 -- remove the prefix LGA from lga_code
SELECT SUBSTRING(lga_code_2016, 4) AS lga_code,
   median_age_persons,
	median_mortgage_repay_monthly,
	median_tot_prsnl_inc_weekly,
	median_rent_weekly,
	median_tot_fam_inc_weekly,
	average_num_psns_per_bedroom,
	median_tot_hhd_inc_weekly,
	average_household_size 
FROM census_g2_stg
