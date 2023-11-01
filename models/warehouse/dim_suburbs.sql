{{
    config(
        target_schema='warehouse',
        materialized='table'
    )
}}

select * from {{ ref('lgasuburb_stg') }}