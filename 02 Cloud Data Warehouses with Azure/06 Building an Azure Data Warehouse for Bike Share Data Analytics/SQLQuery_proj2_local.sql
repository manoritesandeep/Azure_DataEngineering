USE udacity_proj2
GO
/* CREATE TABLES
CREATE TABLE rider (rider_id INTEGER PRIMARY KEY,
					first VARCHAR(50),
					last VARCHAR(50), address VARCHAR(100),
					birthday DATE,
					account_start_date DATE,
					account_end_date DATE,
					is_member VARCHAR(5));

CREATE TABLE payment (payment_id INTEGER PRIMARY KEY,
					  date DATE, 
					  amount MONEY,
					  rider_id INTEGER);
					  
CREATE TABLE station (station_id VARCHAR(50) PRIMARY KEY,
					  name VARCHAR(75), 
					  latitude FLOAT, 
					  longitude FLOAT);
					  
					  
CREATE TABLE trip (trip_id VARCHAR(50) PRIMARY KEY,
				   rideable_type VARCHAR(75),
				   start_at DATETIME,
				   ended_at DATETIME,
				   start_station_id VARCHAR(50),
				   end_station_id VARCHAR(50),
				   rider_id INTEGER);		
				   
-- DROP TABLE trip;
-- TRUNCATE TABLE payment;
*/


/*
SELECT	
	DATEDIFF(MINUTE, start_at, ended_at) as ride_time
FROM trip
*/

/*
-- CREATE STAR SCHEMA TABLES

CREATE TABLE [dbo].[dimDate]
(
  date_key      integer NOT NULL PRIMARY KEY,
  date          date NOT NULL,
  year          smallint NOT NULL,
  quarter       smallint NOT NULL,
  month         smallint NOT NULL,
  day           smallint NOT NULL,
  week          smallint NOT NULL,
  is_weekend	BIT
);
-- DROP TABLE dbo.dimRiders;
CREATE TABLE dbo.dimRiders
(
  rider_key				INT PRIMARY KEY,
  rider_id				int NOT NULL,
  first					varchar(100) NOT NULL,
  last					varchar(100) NOT NULL,
  address				varchar(50) NOT NULL,
  birthday				date,  
  account_start_date    date NOT NULL,
  account_end_date      date NULL,
  is_member				VARCHAR(10)
);
-- DROP TABLE dbo.dimStation;
CREATE TABLE dbo.dimStation
(
  station_key         varchar(50) PRIMARY KEY,
  station_id          varchar(50) NOT NULL,
  name                varchar(255) NOT NULL,
  lattitude           float,
  longitude           float
);
-- DROP TABLE dbo.dimTrips;
CREATE TABLE dbo.dimTrips
(
  trip_key           varchar(255) PRIMARY KEY,
  ride_id            varchar(255) NOT NULL,
  rideable_type      varchar(50) NOT NULL,
  started_at         DATETIME NOT NULL,
  ended_at           DATETIME NOT NULL,
  start_station_id   varchar(50) NOT NULL,
  end_station_id     varchar(50) NOT NULL,
  rider_id           int NOT NULL
);
-- DROP TABLE dbo.fact_payment
CREATE TABLE dbo.fact_payment
(
  payment_key      INT,
  date_key         INT NOT NULL REFERENCES dbo.dimDate(date_key),
  rider_key        INT NOT NULL REFERENCES dbo.dimRiders(rider_key),
  station_key      varchar(50) NOT NULL REFERENCES dbo.dimStation(station_key),
  trip_key         varchar(255) NOT NULL REFERENCES dbo.dimTrips(trip_key),
  amount           decimal(5,2) NOT NULL
);


-- Insert values in about tables...
-- TRUNCATE TABLE [dbo].[dimDate]
INSERT INTO [dbo].[dimDate]
SELECT DISTINCT(cast(FORMAT(date,'yyyyMMdd') as int)) AS date_key,
       FORMAT(date,'yyyyMMdd')         AS date,
       DATEPART(YEAR, date)            AS year,
       DATEPART(quarter, date)         AS quarter,
       DATEPART(month, date)           AS month,
       DAY(date)					   AS day,
       DATEPART(week, date)            AS week,
       CASE WHEN DATEPART(isowk, date) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend
FROM [dbo].[payment]

-- SELECT * FROM [dbo].[dimDate]
-- SET IDENTITY_INSERT [dbo].[dimRiders] OFF
INSERT INTO [dbo].[dimRiders] 
SELECT  r.rider_id as rider_key,
		r.rider_id,
		r.first,
		r.last,
		r.address,
		r.birthday,
		r.account_start_date,
		-- COALESCE(r.account_end_date, NULL) as account_end_date, 
		r.account_end_date,
		r.is_member
FROM [dbo].[rider] r

-- SELECT * FROM [dbo].[dimRiders] 

INSERT INTO dbo.dimStation
SELECT 
		s.station_id as station_key,
		s.station_id,
		s.name,
		s.latitude,
		s.longitude
FROM [dbo].[station] s;

INSERT INTO dbo.dimTrips
SELECT 
		t.trip_id as trip_key,
		t.trip_id,
		t.rideable_type,
		t.start_at,
		t.ended_at,
		t.start_station_id,
		t.end_station_id,
		t.rider_id
FROM [dbo].[trip] t;


INSERT INTO dbo.fact_payment
 SELECT 
        p.payment_id as payment_key,
        cast(FORMAT(date,'yyyyMMdd') as int) as date_key
        -- CAST(p.date AS DATE) as date_key
        , r.rider_id as rider_key
        , s.station_id as station_key
        , t.trip_id as trip_key
        , p.amount
FROM [dbo].[payment] p
    INNER JOIN [dbo].[rider] r ON p.rider_id = r.rider_id
    INNER JOIN [dbo].[trip] t    ON t.rider_id = r.rider_id
    INNER JOIN [dbo].[station] s ON s.station_id = t.start_station_id;

*/
-- SELECT TOP 10 * FROM dbo.fact_payment

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
		dt.day as day_of_week
		-- , CONVERT(TIME, started_at) AS time_of_day
		, CASE 
			WHEN DATEPART(HOUR, CONVERT(TIME, t.started_at)) > 5 AND DATEPART(HOUR, CONVERT(TIME, t.started_at)) < 12 THEN 'morning'
			WHEN DATEPART(HOUR, CONVERT(TIME, t.started_at)) >= 12 AND DATEPART(HOUR, CONVERT(TIME, t.started_at)) < 17 THEN 'afternoon'
			WHEN DATEPART(HOUR, CONVERT(TIME, t.started_at)) >= 17 AND DATEPART(HOUR, CONVERT(TIME, t.started_at)) < 21 THEN 'evening'
			WHEN DATEPART(HOUR, CONVERT(TIME, t.started_at)) >= 21 AND DATEPART(HOUR, CONVERT(TIME, t.started_at)) < 4 THEN 'morning'
			ELSE 'NA'
		END AS time_of_day
		, DATEDIFF(MINUTE, started_at, ended_at) AS ride_time
	FROM fact_payment fp
		INNER JOIN dimTrips t 
			ON fp.trip_key = t.trip_key
		INNER JOIN dimDate dt 
			ON fp.date_key = dt.date_key

)

SELECT TOP 50
	day_of_week,
    time_of_day,
    ride_time AS 'time_per_ride(min)',
    COUNT(ride_time) as cnt_time_per_ride
FROM cte
GROUP BY day_of_week, time_of_day, ride_time;


-- Analyze how much time is spent per ride,
--	Based on which station is the starting and / or ending station
WITH cte as 
(
		SELECT
		t.start_station_id as starting_station,
		t.end_station_id as end_station
		, DATEDIFF(MINUTE, started_at, ended_at) AS ride_time
	FROM fact_payment fp
		INNER JOIN dimTrips t 
			ON fp.trip_key = t.trip_key
		INNER JOIN dimDate dt 
			ON fp.date_key = dt.date_key

)

SELECT TOP 50
	starting_station,
    end_station,
    ride_time AS 'time_per_ride(min)',
    COUNT(ride_time) as cnt_time_per_ride
FROM cte
GROUP BY starting_station, end_station, ride_time
ORDER BY 4 DESC


-- Analyze how much time is spent per ride,
--	   Based on age of the rider at time of the ride
WITH cte as 
(
	SELECT
		DATEDIFF(YEAR, r.birthday, t.started_at) as age
		, DATEDIFF(MINUTE, started_at, ended_at) AS ride_time
	FROM fact_payment fp
		INNER JOIN dimTrips t 
			ON fp.trip_key = t.trip_key
		INNER JOIN dbo.dimRiders r
			ON fp.rider_key = r.rider_key
/* ROUGH work
		SELECT TOP 20
	r.birthday,
	t.started_at,
	DATEDIFF(YEAR,r.birthday, t.started_at) as age
FROM fact_payment fp
	INNER JOIN dbo.dimRiders r
		ON fp.rider_key = r.rider_key
	INNER JOIN dimTrips t 
		ON t.trip_key = fp.trip_key
*/
)

SELECT TOP 50
	age,
    ride_time AS 'time_per_ride(min)',
    COUNT(ride_time) as cnt_time_per_ride
FROM cte
GROUP BY age, ride_time
ORDER BY 1 ASC, 2 DESC, 3 DESC





-- Analyze how much time is spent per ride,
--     Based on whether the rider is a member or a casual rider
WITH cte as 
(
	SELECT
		r.is_member as is_memberz,
		r.rider_id as rider_id
		, DATEDIFF(MINUTE, started_at, ended_at) AS ride_time
	FROM fact_payment fp
		INNER JOIN dimTrips t 
			ON fp.trip_key = t.trip_key
		INNER JOIN dbo.dimRiders r
			ON fp.rider_key = r.rider_key
/* ROUGH work
		SELECT TOP 20
	r.birthday,
	t.started_at,
	DATEDIFF(YEAR,r.birthday, t.started_at) as age
FROM fact_payment fp
	INNER JOIN dbo.dimRiders r
		ON fp.rider_key = r.rider_key
	INNER JOIN dimTrips t 
		ON t.trip_key = fp.trip_key
*/
)

SELECT TOP 50
	is_memberz,
    ride_time AS 'time_per_ride(min)',
    COUNT(ride_time) as cnt_time_per_ride
FROM cte
GROUP BY is_memberz, ride_time
ORDER BY 2 DESC

-- 2. Analyze how much money is spent
--		Per month, quarter, year
SELECT 
	dt.year,
	dt.quarter,
	dt.month
	, sum(fp.amount) as money_spent
FROM fact_payment fp 
	INNER JOIN dimDate dt
		ON fp.date_key = dt.date_key
GROUP BY dt.month, dt.quarter, dt.year
ORDER BY 1, 3, 2, 4 DESC

-- 2. Analyze how much money is spent
--		Per member, based on the age of the rider at account start
WITH cte AS 
(
	SELECT
		r.rider_id as rider_id,
		DATEDIFF(YEAR, r.birthday, t.started_at) as age
		, fp.amount as amount
	FROM fact_payment fp 
		INNER JOIN dimDate dt
			ON fp.date_key = dt.date_key
		INNER JOIN dimTrips t 
				ON fp.trip_key = t.trip_key
			INNER JOIN dbo.dimRiders r
				ON fp.rider_key = r.rider_key
)
SELECT 
	rider_id,
	age,
	SUM(amount) as money_spent
FROM cte
GROUP BY rider_id, age
ORDER BY 2, 3 DESC



-- 3. EXTRA CREDIT - Analyze how much money is spent per member
--	    Based on how many rides the rider averages per month
SELECT 
	DISTINCT t.rider_id as rider,
	dt.month
	, COUNT(*) as num_of_rides
	, SUM(fp.amount) AS total_spent_monthly
	--, SUM(fp.amount)/COUNT(*) as avg_amount -- 356790
	 , AVG(fp.amount) as avg_spent_amt_per_ride
FROM fact_payment fp
	INNER JOIN dimTrips t
		ON t.trip_key = fp.trip_key
	INNER JOIN dimDate dt
			ON fp.date_key = dt.date_key
GROUP BY t.rider_id, dt.month
ORDER BY 3 DESC, 4 DESC, 5 DESC


-- 3. EXTRA CREDIT - Analyze how much money is spent per member
--	    Based on how many minutes the rider spends on a bike per month
SELECT 
	DISTINCT t.rider_id as rider,
	dt.month
	, SUM(DATEDIFF(MINUTE, t.started_at, t.ended_at)) AS min_on_bike
	, COUNT(*) as num_of_rides
	, SUM(fp.amount) AS total_spent_monthly
	--, SUM(fp.amount)/COUNT(*) as avg_amount -- 356790
	 , AVG(fp.amount) as avg_spent_amt_per_ride
FROM fact_payment fp
	INNER JOIN dimTrips t
		ON t.trip_key = fp.trip_key
	INNER JOIN dimDate dt
			ON fp.date_key = dt.date_key
GROUP BY t.rider_id, dt.month
ORDER BY 1 ASC, 2 ASC, 3 DESC, 4 DESC, 5 DESC, 6 DESC


