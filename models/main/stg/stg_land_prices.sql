{{ config(materialized='view') }}

select
    point_id,
    land_price_type,
    case land_price_type
        when 0 then '地価公示'
        when 1 then '地価調査'
    end as land_price_type_name,
    _year as year,
    standard_lot_number_ja as standard_lot_number,
    prefecture_code,
    prefecture_name_ja as prefecture_name,
    city_code,
    ward_town_village_name_ja as municipality_name,
    place_name_ja as place_name,
    location,
    try_cast(regexp_replace(u_current_years_price_ja, '[^0-9]', '', 'g') as bigint) as current_price,
    try_cast(last_years_price as bigint) as last_year_price,
    try_cast(year_on_year_change_rate as double) as change_rate,
    try_cast(regexp_replace(u_cadastral_ja, '[^0-9]', '', 'g') as bigint) as cadastral_area,
    use_category_name_ja as use_category,
    area_division_name_ja as area_division,
    usage_status_name_ja as usage_status,
    current_usage_status_of_surrounding_land_name_ja as surrounding_usage,
    building_structure_name_ja as building_structure,
    u_ground_hierarchy_ja as ground_floors,
    u_underground_hierarchy_ja as underground_floors,
    nearest_station_name_ja as nearest_station,
    u_road_distance_to_nearest_station_name_ja as station_distance,
    front_road_condition,
    try_cast(front_road_width as double) as front_road_width,
    regulations_use_category_name_ja as zoning_use_category,
    try_cast(regexp_replace(u_regulations_building_coverage_ratio_ja, '[^0-9]', '', 'g') as integer) as building_coverage_ratio,
    try_cast(regexp_replace(u_regulations_floor_area_ratio_ja, '[^0-9]', '', 'g') as integer) as floor_area_ratio,
    regulations_fireproof_name_ja as fireproof_category,
    gas_supply_availability,
    water_supply_availability,
    sewer_supply_availability,
    try_cast(longitude as double) as longitude,
    try_cast(latitude as double) as latitude,
    st_geomfromgeojson(geometry) as geom
from {{ ref('raw_land_prices') }}
