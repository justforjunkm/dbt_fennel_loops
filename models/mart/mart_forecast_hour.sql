-- 3. In your mart directory create a file mart_forecast_hour.sql
    -- Goal: A table showing hourly metrics, including location and geo information. For urls in condition_icon column we need a markdown version as a new column.

-- CTE steps:

    -- join prep_forecast_day and staging_location (use Jinja Syntax like in prep models)
    -- add condition_icon_md column with markdown for showing icons (use CONCAT function)
    -- order relevant fields

---------------------------------------------------------------------------------------------------------------
	-- check hourly tables:
/*
select * from prep_forecast_hour;

---------------------------------------------------------------------------------------------------------------
	-- joining again: 
select * 
from prep_forecast_hour
left join staging_location
using (city, region, country);

	-- for sql code with dbt using jinja: 
SELECT * 
FROM {{ref('prep_forecast_day')}}
LEFT JOIN {{ref('staging_location')}}
USING (city, region, country);

---------------------------------------------------------------------------------------------------------------

-- for dbeaver: 

with joining_hour_location as
(
			select * 
			from prep_forecast_hour
			left join staging_location
			using (city, region, country)
),

adding_features as
(
        select *,
            CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') as condition_icon_md
        from joining_hour_location
),

filtering_ordering_features as 
(
        select
            date
            ,city
            ,region
            ,country
            ,time_epoch
            ,date_time
            ,is_day
            ,time
            ,hour
            ,month_of_year
            ,day_of_week
            ,condition_text
            ,condition_icon
            ,condition_icon_md
            ,condition_code
            ,temp_c
            ,wind_kph
            ,wind_degree
            ,wind_dir
            ,pressure_mb
            ,precip_mm
            ,snow_cm
            ,humidity
            ,cloud
            ,feelslike_c
            ,windchill_c
            ,heatindex_c
            ,dewpoint_c
            ,will_it_rain
            ,chance_of_rain
            ,will_it_snow
            ,chance_of_snow
            ,vis_km
            ,gust_kph
            ,uv
         from adding_features
)

select * from filtering_ordering_features;

*/

---------------------------------------------------------------------------------------------------------------

-- for dbt:

with joining_hour_location as
(
        SELECT * 
        FROM {{ref('prep_forecast_hour')}}   
        LEFT JOIN {{ref('staging_location')}}
        USING (city, region, country)
),

adding_features as
(
        select *,
            CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') as condition_icon_md
        from joining_hour_location
),

filtering_ordering_features as 
(
        select
            date
            ,city
            ,region
            ,country
            ,time_epoch
            ,date_time
            ,is_day
            ,time
            ,hour
            ,month_of_year
            ,day_of_week
            ,condition_text
            ,condition_icon
            ,condition_icon_md
            ,condition_code
            ,temp_c
            ,wind_kph
            ,wind_degree
            ,wind_dir
            ,pressure_mb
            ,precip_mm
            ,snow_cm
            ,humidity
            ,cloud
            ,feelslike_c
            ,windchill_c
            ,heatindex_c
            ,dewpoint_c
            ,will_it_rain
            ,chance_of_rain
            ,will_it_snow
            ,chance_of_snow
            ,vis_km
            ,gust_kph
            ,uv
         from adding_features
)

select * from filtering_ordering_features;