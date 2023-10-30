-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view'
        )
}}

-- Create a CTE to select data from the "property_snapshot" reference
WITH property_stg  AS (
    SELECT * FROM {{ ref('property_snapshot') }}
)

-- Select all records from the property_stg CTE
SELECT * FROM property_stg