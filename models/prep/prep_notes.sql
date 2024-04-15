-- working with prep to see what is possible

-------------- JOIN --------------

-- looking in to the data to see what type of join we need and which columns to join on:
select * from prep_forecast_day;
select * from staging_location;
-- overlapping columns: city, region, country
-- left join would be best, where prep is the left table

SELECT * 
FROM prep_forecast_day
LEFT JOIN staging_location
USING (city, region, country);

-- for sql code with dbt: 
SELECT * 
FROM {{ref('prep_forecast_day')}}
LEFT JOIN {{ref('staging_location')}}
USING (city, region, country);


-------------- AS --------------
-- looking into the whole data:
select * from prep_forecast_day;

-- looking into the weeks:
select week_of_year as cw  -- cw stands for calendar week
from prep_forecast_day;


----------- CASE WHEN -----------
-- looking into the conditions of the day like (sunny, cloudy, partly cloudy, moderate rain at times etc.):
select distinct condition_text
from prep_forecast_day;

-- what if we want to see buckets for these conditions? 
select                  			-- case statement has: case when ... end
		case when condition_text = 'Sunny' then 1 else 0 end as sunny_days,  -- sunny_days is an alias , giving the column a name, we make a column now where it's boolean, 1 if the day's condition is in the sunny basket 
		case when condition_text in 
									('Cloudy',
									'Partly cloudy',
									'Overcast',
									'Mist'
									'Fog',
									'Freezing fog') then 1 else 0 end as other_days,
		case when condition_text in 
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
									'Thundery outbreaks possible') then 1 else 0 end as rainy_days,
		case when condition_text in 
									('Moderate snow',
									'Light snow',
									'Moderate or heavy snow showers',
									'Light snow showers',
									'Patchy moderate snow',
									'Heavy snow',
									'Blowing snow',
									'Patchy light snow',
									'Light sleet') then 1 else 0 end as snowy_days
from prep_forecast_day;
		


-- now let's sum the sunny, cloudy, snowy, other days for each month:
select year, month_of_year, city,            			-- case statement has: (case when ... end)
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
from prep_forecast_day
group by (year, month_of_year, city)
order by year, month_of_year;




-------------- IN --------------


------------- CAST -------------

-- notice in the moonrise columns there are cells that have no moonrise:
select moonrise from prep_forecast_day;

-- want to cast this column as time:
select moonrise::time, 
 cast (moonrise, as time)
 from prep_forecast_day; -- gives an error > because 'no moonrise' is a string and cannot be casted as time, so first take care of that
 
 -- let's put null-values for the no moonrise cells
select (case when moonrise = 'No moonrise' then null else moonrise end)
from prep_forecast_day;  -- if it finds 'no moonrise' make it null values, else it should keep the value of the moonrise


select -- *,
		(case when moonrise = 'No moonrise' then null else moonrise end)::time moonrise_n -- makes a new column
		,cast((case when moonrise = 'No moonrise' then null else moonrise end) as time)  -- another way, but casting the actual moonrise column
from prep_forecast_day; 


----------- CONCAT() -----------

select condition_icon from prep_forecast_day;  -- > you can put this in google, it's a url showing an icon > test with https://cdn.weatherapi.com/weather/64x64/day/122.png

-- in jupyter notebook you can display this icon with markdown:
-- ![weather-icon](//cdn.weatherapi.com/weather/64x64/day/122.png)
-- format : ![name](url-linkj)  jupyter notebook is using markdown and translating it to html
-- you can display it smaller by putting ?width=35 in the url > ![weather-icon](//cdn.weatherapi.com/weather/64x64/day/122.png?width=35)

-- using this we can change all the strings in the condition_icon column to these pictures

select 
		concat('![weather-icon](',condition_icon,'?width=35')  -- here we are just concatenating 2 strings together
from prep_forecast_day;

-- now we can save it as a new column in the table:
select *,
		concat('![weather-icon](',condition_icon,'?width=35') as icon_markdown -- here we are just concatenating 2 strings together
from prep_forecast_day;


-- let's see how many different icons we have:
select distinct 
		concat('![weather-icon](',condition_icon,'?width=35')  -- here we are just concatenating 2 strings together
from prep_forecast_day; -- 33 different icons, we could group these together and use it for certain graphs
