{{
    config(
        target_schema='warehouse',
        materialized='table'
    )
}}

select * from {{ ref('room_stg') }}