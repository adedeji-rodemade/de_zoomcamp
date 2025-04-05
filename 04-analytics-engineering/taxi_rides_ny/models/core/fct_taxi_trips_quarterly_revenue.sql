{{
    config(
        materialized='table'
    )
}}

With trip_agg as (
    select 
        service_type,
        year_quarter,
        sum(total_amount) as quarterly_revenue 
    from {{ ref('fact_trips') }}
)