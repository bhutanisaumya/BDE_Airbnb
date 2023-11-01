{{
    config(
        target_schema='warehouse',
        materialized='table'
    )
}}

select * from {{ ref('host_stg') }}