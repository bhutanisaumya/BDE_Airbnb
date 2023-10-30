-- Define a snapshot named "room_snapshot"
{% snapshot room_snapshot %}
{{
    config(
      target_schema='raw',
      materialized='snapshot',
      strategy='timestamp',
      unique_key="host_id",
      updated_at='scraped_date'
    )
}}
-- Create a CTE to select room data  
with room_data as (
    select DISTINCT
        host_id,
        listing_id,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        scraped_date
    from {{ source('raw', 'listings') }}
)
-- Select data from the CTE inside the snapshot block
select * from room_data
{% endsnapshot %}
