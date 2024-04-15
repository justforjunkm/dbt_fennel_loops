-- 1. In your mart directory create a file mart_conditions_week.sql that will create your first Data Mart.
    -- Goal: A table showing weekly averages and total counts, including location and geo information.

-- CTE steps:

    -- join prep_forecast_day and staging_location (use Jinja Syntax like in prep models)
    -- select relevant fields for grouping and aggregation (no group bu yet)
    -- add buckets for sunny_days, rainy_days, snowy_days and other_days and aggregate metrics per week per location using GROUP BY

-- Hints:
    -- first find all unique values of condition_text in order to define buckets
    -- use CASE WHEN ... THEN ... ELSE ... END statements.
    -- depending on which dimension columns you want to keep, it will require 6-9 fields for the GROUP BY

-- weekly aggregation of the conditions, weather, humidity, windspeed

/*
SELECT * 
FROM prep_forecast_day
LEFT JOIN staging_location
USING (city, region, country);

-- for sql code with dbt using jinja: 
SELECT * 
FROM {{ref('prep_forecast_day')}}
LEFT JOIN {{ref('staging_location')}}
USING (city, region, country);

-------------------------------------------------------------------------------------------------------------

-- selecting the columns for aggregations:
with agg_columns as (
			select		
			year_and_week
            ,week_of_year
            ,year
            ,city
            ,region
            ,country
            --,lat   -- commented out because we are just selecting from prep, lat, lon and timezone_id are in staging_locations
            --,lon
            --,timezone_id
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
            -- ,condition_icon
            -- ,condition_code
            -- ,max_wind_kph
            -- ,avg_vis_km
            -- ,uv
            -- ,sunrise
            -- ,sunset
            -- ,moonrise
            -- ,moonset
            -- ,moon_phase
            -- ,moon_illumination
            -- ,day_of_month
            -- ,month_of_year
            -- ,day_of_week
			from prep_forecast_day)

select * from agg_columns;
*/

-------------------------------------------------------------------------------------------------------------
/*
            -- sql code for dbt: 

with joining_day_location as 
(
			select * 
			from prep_forecast_day
			left join staging_location
			using (city, region, country) 
),

filtering_columns as 
(
			select		
			year_and_week
            ,week_of_year
            ,year
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
            -- ,condition_icon
            -- ,condition_code
            -- ,max_wind_kph
            -- ,avg_vis_km
            -- ,uv
            -- ,sunrise
            -- ,sunset
            -- ,moonrise
            -- ,moonset
            -- ,moon_phase
            -- ,moon_illumination
            -- ,day_of_month
            -- ,month_of_year
            -- ,day_of_week
			from joining_day_location
)

select * from filtering_columns;

    -------------------------------------------------------------------------------------------------------------

-- now with aggregrate, for dbeaver:


with joining_day_location as 
(
			select * 
			from prep_forecast_day
			left join staging_location
			using (city, region, country) 
),

filtering_columns as 
(
			select		
			year_and_week
            ,week_of_year
            ,year
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
            -- ,condition_icon
            -- ,condition_code
            -- ,max_wind_kph
            -- ,avg_vis_km
            -- ,uv
            -- ,sunrise
            -- ,sunset
            -- ,moonrise
            -- ,moonset
            -- ,moon_phase
            -- ,moon_illumination
            -- ,day_of_month
            ,month_of_year
            -- ,day_of_week
			from joining_day_location
),

aggregation_week as 
(
			select		
            year_and_week  -- grouping on
            ,week_of_year   -- grouping on
            ,year           -- grouping on
            ,city           -- grouping on
            ,region         -- grouping on
            ,country        -- grouping on
            ,lat            -- grouping on
            ,lon            -- grouping on
            ,timezone_id    -- grouping on
            ,month_of_year
            ,MAX(max_temp_c) AS max_temp_c
            ,MIN(min_temp_c) AS min_temp_c
            ,AVG(avg_temp_c) AS avg_temp_c
            ,SUM(total_precip_mm) AS total_precip_mm
            ,SUM(total_snow_cm) AS total_snow_cm
            ,AVG(avg_humidity) AS avg_humidity
            ,SUM(daily_will_it_rain) AS will_it_rain_days
            ,AVG(daily_chance_of_rain) AS daily_chance_of_rain_avg
            ,SUM(daily_will_it_snow) AS will_it_snow_days
            ,AVG(daily_chance_of_snow) AS daily_chance_of_snow_avg,
			sum(case when condition_text = 'Sunny' then 1 else 0 end) as sunny_days,  -- sunny_days is an alias , giving the column a name, we make a column now where it's boolean, 1 if the day's condition is in the sunny basket 
			sum(case when condition_text in 
									('Cloudy',
									'Partly cloudy',
									'Overcast',
									'Mist'
									'Fog',
									'Freezing fog') then 1 else 0 end) as other_days,
			sum(case when condition_text in 
									('Heavy rain at times',
									'Moderate rain',
									'Patchy rain possible',
									'Light drizzle',
									'Heavy rain',
									'Moderate or heavy rain shower',
									'Light rain',
									'Patchy light drizzle',
									'Moderate rain at times',
									'Light freezing rain',
									'Patchy light rain with thunder',
									'Patchy light rain',
									'Light rain shower',
									'Moderate or heavy rain with thunder',
									'Thundery outbreaks possible') then 1 else 0 end) as rainy_days,
			sum(case when condition_text in 
									('Moderate snow',
									'Light snow',
									'Moderate or heavy snow showers',
									'Light snow showers',
									'Patchy moderate snow',
									'Heavy snow',
									'Blowing snow',
									'Patchy light snow',
									'Light sleet') then 1 else 0 end) as snowy_days
			from filtering_columns
			group by 
									(year_and_week  
						            ,week_of_year  
						            ,year           
						            ,city           
						            ,region         
						            ,country        
						            ,lat
						            ,lon
						            ,timezone_id
                                    ,month_of_year)   
			order by city
)

select * from aggregation_week;

*/
-------------------------------------------------------------------------------------------------------------


-- sql code for dbt: 


with joining_day_location as 
(
			SELECT * 
			FROM {{ref('prep_forecast_day')}}
			LEFT JOIN {{ref('staging_location')}}
			USING (city, region, country)
),

filtering_columns as 
(
			select		
			year_and_week
            ,week_of_year
            ,year
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
            -- ,condition_icon
            -- ,condition_code
            -- ,max_wind_kph
            -- ,avg_vis_km
            -- ,uv
            -- ,sunrise
            -- ,sunset
            -- ,moonrise
            -- ,moonset
            -- ,moon_phase
            -- ,moon_illumination
            -- ,day_of_month
            ,month_of_year
            -- ,day_of_week
			from joining_day_location
),

aggregation_week as 
(
			select		
            year_and_week  -- grouping on
            ,week_of_year   -- grouping on
            ,year           -- grouping on
            ,city           -- grouping on
            ,region         -- grouping on
            ,country        -- grouping on
            ,lat            -- grouping on
            ,lon            -- grouping on
            ,timezone_id    -- grouping on
            ,month_of_year
            ,MAX(max_temp_c) AS max_temp_c
            ,MIN(min_temp_c) AS min_temp_c
            ,AVG(avg_temp_c) AS avg_temp_c
            ,SUM(total_precip_mm) AS total_precip_mm
            ,SUM(total_snow_cm) AS total_snow_cm
            ,AVG(avg_humidity) AS avg_humidity
            ,SUM(daily_will_it_rain) AS will_it_rain_days
            ,AVG(daily_chance_of_rain) AS daily_chance_of_rain_avg
            ,SUM(daily_will_it_snow) AS will_it_snow_days
            ,AVG(daily_chance_of_snow) AS daily_chance_of_snow_avg,
			sum(case when condition_text = 'Sunny' then 1 else 0 end) as sunny_days,  -- sunny_days is an alias , giving the column a name, we make a column now where it's boolean, 1 if the day's condition is in the sunny basket 
			sum(case when condition_text in 
									('Cloudy',
									'Partly cloudy',
									'Overcast',
									'Mist'
									'Fog',
									'Freezing fog') then 1 else 0 end) as other_days,
			sum(case when condition_text in 
									('Heavy rain at times',
									'Moderate rain',
									'Patchy rain possible',
									'Light drizzle',
									'Heavy rain',
									'Moderate or heavy rain shower',
									'Light rain',
									'Patchy light drizzle',
									'Moderate rain at times',
									'Light freezing rain',
									'Patchy light rain with thunder',
									'Patchy light rain',
									'Light rain shower',
									'Moderate or heavy rain with thunder',
									'Thundery outbreaks possible') then 1 else 0 end) as rainy_days,
			sum(case when condition_text in 
									('Moderate snow',
									'Light snow',
									'Moderate or heavy snow showers',
									'Light snow showers',
									'Patchy moderate snow',
									'Heavy snow',
									'Blowing snow',
									'Patchy light snow',
									'Light sleet') then 1 else 0 end) as snowy_days
			from filtering_columns
			group by 
									(year_and_week  
						            ,week_of_year  
						            ,year           
						            ,city           
						            ,region         
						            ,country        
						            ,lat
						            ,lon
						            ,timezone_id
                                    ,month_of_year)   
			order by city
)

select * from aggregation_week