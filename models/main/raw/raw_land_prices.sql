{{ config(materialized='view') }}

select *
from {{ source('reinfolib_source', 'land_prices') }}
