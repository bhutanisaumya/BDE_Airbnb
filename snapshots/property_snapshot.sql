-- Define a snapshot named "property_snapshot"
{% snapshot property_snapshot %}
    {{
        config(
          target_schema='raw',
          materialized='snapshot',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='scraped_date',
        )
    }}
  -- Create a CTE to select property data  
    with property_data as (
      select  DISTINCT
             listing_id,
             host_id,
             listing_neighbourhood,
             property_type, 
             room_type,
             accommodates,
             scraped_date 
      from {{ source('raw', 'listings') }}
    )
    
    -- Select data from the CTE inside the snapshot block
    select * from property_data
{% endsnapshot %}
