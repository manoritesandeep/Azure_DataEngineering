/*
The business outcomes you are designing for are as follows:
    Analyze how much time is spent per ride
        Based on date and time factors such as day of week and time of day
        Based on which station is the starting and / or ending station
        Based on age of the rider at time of the ride
        Based on whether the rider is a member or a casual rider
*/

-- Analyze how much time is spent per ride,
--  Based on date and time factors such as day of week and time of day
WITH cte as  
(
    SELECT
    DATENAME(dw,dt.day) AS day_of_week,
    CASE 
        WHEN DATEPART(HOUR, CONVERT(TIME, t.start_at)) > 0 AND DATEPART(HOUR,CONVERT(TIME, t.start_at)) < 12 THEN 'Morning'
        WHEN DATEPART(HOUR, CONVERT(TIME, t.start_at)) >= 12 AND DATEPART(HOUR,CONVERT(TIME, t.start_at)) < 16 THEN 'Afternoon'
        WHEN DATEPART(HOUR, CONVERT(TIME, t.start_at)) >= 16 AND DATEPART(HOUR,CONVERT(TIME, t.start_at)) < 24 THEN 'Evening'
        ELSE 'NA'
    END  AS time_of_day
    -- , CONVERT(TIME, t.start_at) as sample_test  -- 23:25:40
    , DATEDIFF(MINUTE, t.start_at, t.ended_at) as time_per_ride
    -- AS time_of_day
FROM dbo.fact_payment p
    INNER JOIN dbo.dimDate dt ON p.date_key = dt.date_key
    INNER JOIN dbo.dimTrip t ON p.trip_key = t.trip_key
)
SELECT     
    day_of_week,
    time_of_day,
    time_per_ride AS 'time_per_ride(min)',
    COUNT(time_per_ride) as cnt_time_per_ride
FROM cte
WHERE time_per_ride > 0
GROUP BY  day_of_week, time_of_day, time_per_ride
ORDER BY 2 , 4 DESC

-- Analyze how much time is spent per ride,
--  Based on which station is the starting and / or ending station
SELECT TOP 10 
    s.* 
    , s.latitude
    , ST_DISTANCE
    , DATEDIFF(MINUTE, t.start_at, t.ended_at) as time_per_ride
    -- AS time_of_day
FROM dbo.fact_payment p
    INNER JOIN dbo.dimDate dt ON p.date_key = dt.date_key
    INNER JOIN dbo.dimTrip t ON p.trip_key = t.trip_key
    INNER JOIN dbo.dimStation s ON p.station_key = s.station_key

-- Trial 2
DECLARE @startlocation geography = geography::Point(latitude, longitude, 4326);
DECLARE @endlocation geography = geography::Point(latitude, longitude, 4326);
-- DECLARE @startlocation geometry = geometry::Point(latitude, longitude, 4326);
-- DECLARE @endlocation geometry = geometry::Point(latitude, longitude, 4326);
SELECT TOP 10 
    s.* 
    , @startlocation.STDistance(@endlocation) AS dist_btw_rides
    , DATEDIFF(MINUTE, t.start_at, t.ended_at) as time_per_ride
    -- AS time_of_day
FROM dbo.fact_payment p
    INNER JOIN dbo.dimDate dt ON p.date_key = dt.date_key
    INNER JOIN dbo.dimTrip t ON p.trip_key = t.trip_key
    INNER JOIN dbo.dimStation s ON p.station_key = s.station_key














