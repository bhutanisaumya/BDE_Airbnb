{{
    config(
        target_schema='warehouse',
        materialized='table'
            )
}}

select * from {{ ref('census_g2_stg') }}