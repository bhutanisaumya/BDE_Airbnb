-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view',
       )
}}

-- Create a CTE for the census data from the "census_g1" source
WITH census_g1_stg AS (

    SELECT * FROM {{source('raw','census_g1')}}

)

-- Select specific columns
SELECT SUBSTRING(lga_code_2016, 4) AS lga_code,
    tot_p_m,
	tot_p_f,
	tot_p_p ,
	age_0_4_yr_m,
	age_0_4_yr_f,
	age_0_4_yr_p,
	age_5_14_yr_m,
	age_5_14_yr_f,
	age_5_14_yr_p ,
	age_15_19_yr_m,
	age_15_19_yr_f,
	age_15_19_yr_p,
	age_20_24_yr_m,
	age_20_24_yr_f,
	age_20_24_yr_p,
	age_25_34_yr_m,
	age_25_34_yr_f,
	age_25_34_yr_p,
	age_35_44_yr_m,
	age_35_44_yr_f,
	age_35_44_yr_p,
	age_45_54_yr_m,
	age_45_54_yr_f,
	age_45_54_yr_p,
	age_55_64_yr_m,
	age_55_64_yr_f,
	age_55_64_yr_p,
	age_65_74_yr_m,
	age_65_74_yr_f,
	age_65_74_yr_p,
	age_75_84_yr_m,
	age_75_84_yr_f,
	age_75_84_yr_p,
	age_85ov_m ,
	age_85ov_f ,
	age_85ov_p 	
FROM census_g1_stg
