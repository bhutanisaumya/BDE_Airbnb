{{
    config(
        target_schema='warehouse',
        materialized='table'
    )
}}

select * from {{ ref('lgacode_stg') }}