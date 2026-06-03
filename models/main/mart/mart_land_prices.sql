{{ config(materialized='table') }}

select * from {{ ref('stg_land_prices') }}
