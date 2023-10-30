-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view'
         )
    }}

-- Create a CTE to select data from the "lgacode" source
WITH lgacode_stg AS (

    SELECT * FROM {{source('raw','lgacode')}}

)

-- Select all records from the lgacode_stg CTE
SELECT * FROM lgacode_stg 