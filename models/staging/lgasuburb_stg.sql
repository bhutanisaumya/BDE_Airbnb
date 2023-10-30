-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view'
        )
}}

-- Create a CTE to select data from the "lgasuburb" source
WITH lgasuburb_stg AS (

    SELECT * FROM {{source('raw','lgasuburb')}}

)

-- Select and format data from the lgasuburb_stg CTE
SELECT 
    INITCAP(lga_name) AS lga_name,
    INITCAP(suburb_name) AS suburb_name
FROM lgasuburb_stg
