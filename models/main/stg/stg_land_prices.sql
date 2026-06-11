{{ config(materialized='view') }}

-- raw_land_prices は properties を JSON 文字列で保持しているため、
-- ここで json_extract により各フィールドを取り出して構造化する。
select
    try_cast(json_extract_string(properties, '$.point_id') as bigint) as point_id,
    try_cast(json_extract_string(properties, '$.land_price_type') as integer) as land_price_type,
    case try_cast(json_extract_string(properties, '$.land_price_type') as integer)
        when 0 then '地価公示'
        when 1 then '地価調査'
    end as land_price_type_name,
    _year as year,
    json_extract_string(properties, '$.standard_lot_number_ja') as standard_lot_number,
    json_extract_string(properties, '$.prefecture_code') as prefecture_code,
    json_extract_string(properties, '$.prefecture_name_ja') as prefecture_name,
    json_extract_string(properties, '$.city_code') as city_code,
    json_extract_string(properties, '$.ward_town_village_name_ja') as municipality_name,
    json_extract_string(properties, '$.place_name_ja') as place_name,
    json_extract_string(properties, '$.location') as location,
    try_cast(regexp_replace(json_extract_string(properties, '$.u_current_years_price_ja'), '[^0-9]', '', 'g') as bigint) as current_price,
    try_cast(json_extract_string(properties, '$.last_years_price') as bigint) as last_year_price,
    try_cast(json_extract_string(properties, '$.year_on_year_change_rate') as double) as change_rate,
    try_cast(regexp_replace(json_extract_string(properties, '$.u_cadastral_ja'), '[^0-9]', '', 'g') as bigint) as cadastral_area,
    json_extract_string(properties, '$.use_category_name_ja') as use_category,
    json_extract_string(properties, '$.area_division_name_ja') as area_division,
    json_extract_string(properties, '$.usage_status_name_ja') as usage_status,
    json_extract_string(properties, '$.current_usage_status_of_surrounding_land_name_ja') as surrounding_usage,
    json_extract_string(properties, '$.building_structure_name_ja') as building_structure,
    json_extract_string(properties, '$.u_ground_hierarchy_ja') as ground_floors,
    json_extract_string(properties, '$.u_underground_hierarchy_ja') as underground_floors,
    json_extract_string(properties, '$.nearest_station_name_ja') as nearest_station,
    json_extract_string(properties, '$.u_road_distance_to_nearest_station_name_ja') as station_distance,
    json_extract_string(properties, '$.front_road_condition') as front_road_condition,
    try_cast(json_extract_string(properties, '$.front_road_width') as double) as front_road_width,
    json_extract_string(properties, '$.regulations_use_category_name_ja') as zoning_use_category,
    try_cast(regexp_replace(json_extract_string(properties, '$.u_regulations_building_coverage_ratio_ja'), '[^0-9]', '', 'g') as integer) as building_coverage_ratio,
    try_cast(regexp_replace(json_extract_string(properties, '$.u_regulations_floor_area_ratio_ja'), '[^0-9]', '', 'g') as integer) as floor_area_ratio,
    json_extract_string(properties, '$.regulations_fireproof_name_ja') as fireproof_category,
    try_cast(json_extract_string(properties, '$.gas_supply_availability') as boolean) as gas_supply_availability,
    try_cast(json_extract_string(properties, '$.water_supply_availability') as boolean) as water_supply_availability,
    try_cast(json_extract_string(properties, '$.sewer_supply_availability') as boolean) as sewer_supply_availability,
    try_cast(longitude as double) as longitude,
    try_cast(latitude as double) as latitude,
    st_geomfromgeojson(geometry) as geom
from {{ ref('raw_land_prices') }}
