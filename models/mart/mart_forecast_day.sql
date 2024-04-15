-- 2. In your mart directory create a file mart_forecast_day.sql
    -- Goal: A table showing daily metrics, including location and geo information. 
    -- For urls in condition_icon column we need a markdown version as a new column. Example:
    -- ![weather_icon](//cdn.weatherapi.com/weather/64x64/day/353.png?width=35)


-- The type of the columns sun/moon/set/rise should be TIME.

    -- values like “No moonrise” can be updated as NULL using CASE WHEN ... THEN ... ELSE ... END statements


-- CTE steps:

    -- join prep_forecast_day and staging_location (use Jinja Syntax like in prep models)
    -- add condition_icon_md column with markdown for showing icons (use CONCAT function)
    -- order relevant fields
-------------------------------------------------------------------------------------------------------------
-- joining again: 
/*
select * 
from prep_forecast_day
left join staging_location
using (city, region, country);

-- for sql code with dbt using jinja: 
SELECT * 
FROM {{ref('prep_forecast_day')}}
LEFT JOIN {{ref('staging_location')}}
USING (city, region, country);

-------------------------------------------------------------------------------------------------------------


-- accounting for the No moonrise, moonset, sunrise, sunset values in the moonrise, moonset, sunrise, sunset columns:
select moonrise, 
		moonset,
		sunrise,
		sunset
from prep_forecast_day; 

-- adding new moonrise etc. columns for which the no moonrise because NULL and changing the condition_icon url to markdown readable html
with joining_day_location as 
(
			select * 
			from prep_forecast_day
			left join staging_location
			using (city, region, country)
),

adding_features as 
(
        	select *
            ,CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') AS condition_icon_md
            ,(CASE WHEN moonrise = 'No moonrise' THEN null ELSE moonrise END)::TIME AS moonrise_n
            ,(CASE WHEN moonset = 'No moonset' THEN null ELSE moonset END)::TIME AS moonset_n
            ,(CASE WHEN sunrise = 'No sunrise' THEN null ELSE sunrise END)::TIME AS sunrise_n
            ,(CASE WHEN sunset = 'No sunset' THEN null ELSE sunset END)::TIME AS sunset_n
        	from joining_day_location
)
select * from adding_features;
          


-------------------------------------------------------------------------------------------------------------
-- no with reordering the columns:
       
WITH joining_day_location AS (
			select * 
			from prep_forecast_day
			left join staging_location
			using (city, region, country)
),

adding_features AS (
        SELECT 
            *
            ,CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') AS condition_icon_md
            ,(CASE WHEN moonrise = 'No moonrise' THEN null ELSE moonrise END)::TIME AS moonrise_n
            ,(CASE WHEN moonset = 'No moonset' THEN null ELSE moonset END)::TIME AS moonset_n
            ,(CASE WHEN sunrise = 'No sunrise' THEN null ELSE sunrise END)::TIME AS sunrise_n
            ,(CASE WHEN sunset = 'No sunset' THEN null ELSE sunset END)::TIME AS sunset_n
       		,(CASE WHEN month_of_year IN ('december ', 'january  ', 'february ') THEN 'Winter'
		       WHEN month_of_year IN ('march    ', 'april    ', 'may      ') THEN 'Spring'
		       WHEN month_of_year IN ('june     ', 'july     ', 'august   ') THEN 'Summer'
		       WHEN month_of_year IN ('september', 'october  ', 'november ') THEN 'Fall'
		       ELSE 'Unknown' END) as season
		    , max_temp_c - min_temp_c AS temp_range
		    ,(CASE WHEN day_of_week IN ('saturday ', 'sunday   ') THEN 1 ELSE 0 END) as weekend
        FROM joining_day_location
),

filtering_ordering_features AS (
        SELECT 
            date
            ,day_of_month
            ,month_of_year
            ,year
            ,day_of_week
            ,week_of_year
            ,year_and_week
            ,city
            ,region
            ,country
            ,lat
            ,lon
            ,timezone_id
            ,max_temp_c
            ,min_temp_c
            ,avg_temp_c
            ,total_precip_mm
            ,total_snow_cm
            ,avg_humidity
            ,daily_will_it_rain
            ,daily_chance_of_rain
            ,daily_will_it_snow
            ,daily_chance_of_snow
            ,condition_text
            ,condition_icon
            ,condition_icon_md
            ,condition_code
            ,max_wind_kph
            ,avg_vis_km
            ,uv
            ,sunrise_n
            ,sunset_n
            ,moonrise_n
            ,moonset_n
            ,moon_phase
            ,moon_illumination
            ,sunset_n-sunrise_n AS daylight_hours
            ,season
            ,temp_range
            ,weekend
        FROM adding_features
)

select * from filtering_ordering_features;
*/
-------------------------------------------------------------------------------------------------------------

-- for sql file such that dbt can be run: 

WITH joining_day_location AS (
        SELECT * 
        FROM {{ref('prep_forecast_day')}}
        LEFT JOIN {{ref('staging_location')}}
        USING(city,region,country)
),
adding_features AS (
        SELECT 
            *
            ,CONCAT('&nbsp;&nbsp;&nbsp;&nbsp;![weather_icon](',condition_icon,'?width=35)') AS condition_icon_md
            ,(CASE WHEN moonrise = 'No moonrise' THEN null ELSE moonrise END)::TIME AS moonrise_n
            ,(CASE WHEN moonset = 'No moonset' THEN null ELSE moonset END)::TIME AS moonset_n
            ,(CASE WHEN sunrise = 'No sunrise' THEN null ELSE sunrise END)::TIME AS sunrise_n
            ,(CASE WHEN sunset = 'No sunset' THEN null ELSE sunset END)::TIME AS sunset_n
            ,(CASE WHEN month_of_year IN ('december ', 'january  ', 'february ') THEN 'Winter'
		       WHEN month_of_year IN ('march    ', 'april    ', 'may      ') THEN 'Spring'
		       WHEN month_of_year IN ('june     ', 'july     ', 'august   ') THEN 'Summer'
		       WHEN month_of_year IN ('september', 'october  ', 'november ') THEN 'Fall'
		       ELSE 'Unknown' END) as season
		    , max_temp_c - min_temp_c AS temp_range
		    ,(CASE WHEN day_of_week IN ('saturday ', 'sunday   ') THEN 1 ELSE 0 END) as weekend
        FROM joining_day_location
),
filtering_ordering_features AS (
        SELECT 
            date
            ,day_of_month
            ,month_of_year
            ,year
            ,day_of_week
            ,week_of_year
            ,year_and_week
            ,city
            ,region
            ,country
            ,lat
            ,lon
            ,timezone_id
            ,max_temp_c
            ,min_temp_c
            ,avg_temp_c
            ,total_precip_mm
            ,total_snow_cm
            ,avg_humidity
            ,daily_will_it_rain
            ,daily_chance_of_rain
            ,daily_will_it_snow
            ,daily_chance_of_snow
            ,condition_text
            ,condition_icon
            ,condition_icon_md
            ,condition_code
            ,max_wind_kph
            ,avg_vis_km
            ,uv
            ,sunrise_n
            ,sunset_n
            ,moonrise_n
            ,moonset_n
            ,moon_phase
            ,moon_illumination
            ,sunset_n-sunrise_n AS daylight_hours
            ,season
            ,temp_range
            ,weekend
        FROM adding_features
)
SELECT * FROM filtering_ordering_features