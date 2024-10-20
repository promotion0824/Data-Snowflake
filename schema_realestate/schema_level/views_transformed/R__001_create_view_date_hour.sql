-- ******************************************************************************************************************************
-- Dimension view all dates/hours
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.date_hour AS
	WITH cte_start_of_hour AS (
	SELECT column1 AS hour, column2 AS start_of_hour
	FROM values 
		(0,'00:00'),
		(1,'01:00'),
		(2,'02:00'),
		(3,'03:00'),
		(4,'04:00'),
		(5,'05:00'),
		(6,'06:00'),
		(7,'07:00'),
		(8,'08:00'),
		(9,'09:00'),
		(10,'10:00'),
		(11,'11:00'),
		(12,'12:00'),
		(13,'13:00'),
		(14,'14:00'),
		(15,'15:00'),
		(16,'16:00'),
		(17,'17:00'),
		(18,'18:00'),
		(19,'19:00'),
		(20,'20:00'),
		(21,'21:00'),
		(22,'22:00'),
		(23,'23:00') 
	)
	SELECT 
		date,
		DATEADD(h,hour,date) AS date_time_hour,
		hour AS hour_num,
		day AS day_num,
		day_name,
		month AS month_num,
		month_name AS month_abbrev,
		TO_CHAR(date,'MMMM') AS month_name,
		day_of_week,
		week_of_year,
		day_of_year,
		is_weekday,
		NOT is_weekday AS is_weekend,
		CASE WHEN hour BETWEEN 8 AND 18 THEN 1 ELSE 0 END AS is_business_hour,
		last_day_of_month
	FROM utils.dates d
	CROSS JOIN cte_start_of_hour h
;

