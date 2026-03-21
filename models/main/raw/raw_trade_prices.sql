{{ config(materialized='view') }}

select *
from {{ source('reinfolib_source', 'trade_prices') }}
