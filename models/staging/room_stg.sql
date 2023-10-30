-- Set the target schema and materialization type
{{
    config(
        target_schema='staging',
        materialized='view'
        )
}}

-- Create a CTE to select data from the "room_snapshot" reference
WITH room_stg  AS (
    SELECT * FROM {{ ref('room_snapshot') }}
)

-- Select all records from the room_stg CTE
SELECT * FROM room_stg