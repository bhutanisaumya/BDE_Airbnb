{{
    config(
        target_schema='warehouse',
        materialized='table'       
    )
}}

select * from {{ ref('census_g1_stg') }}