{{
    config(
        target_schema='warehouse',
        materialized='table'
    )
}}

select * from {{ ref('property_stg') }}