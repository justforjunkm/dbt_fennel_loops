WITH forecast_hour_data AS (    -- using common table expressions
    SELECT * 
    FROM {{ref('staging_forecast_hour')}}  
),
add_features AS (
    SELECT *
        ,date_time::time AS time -- only time (hours:minutes:seconds) as TIME data type, from the column date_time we cast (:: double colon means casting a datatype) it as time and give it an alias time
        ,TO_CHAR(date_time,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type, to_char means it will be a string, using the date_time column we change it to 24 hous and minutes format
        ,TO_CHAR(date_time, 'month') AS month_of_year -- month name as a text
        ,TO_CHAR(date_time, 'day') AS day_of_week -- weekday name as text
    FROM forecast_hour_data
)
SELECT * FROM add_features;

