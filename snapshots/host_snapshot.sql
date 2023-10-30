-- Define a snapshot named "host_snapshot"
{% snapshot host_snapshot %}
    {{
        config(
          target_schema='raw',
          materialized='snapshot',
          strategy='timestamp',
          unique_key='host_id',
          updated_at='scraped_date',
        )
    }}
    -- Create a CTE to select host data  
    with host_data as (
      select DISTINCT
              host_id,
              host_name, 
              host_since,
              host_is_superhost,
              host_neighbourhood,
              scraped_date 
      from {{ source('raw', 'listings') }}
    )

    -- Select data from the CTE inside the snapshot block
    select * from host_data

{% endsnapshot %}

