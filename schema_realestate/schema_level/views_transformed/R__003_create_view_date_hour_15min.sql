-- ******************************************************************************************************************************
-- Dimension view all dates/hours/15 mins
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW transformed.date_hour_15min AS
	WITH cte_15min AS (
	SELECT column1 AS minute
	FROM values 
		(0),
		(15),
		(30),
		(45)
	)
	SELECT 
		date,
		date_time_hour AS date_time_local_hour,
        DATEADD(m,minute,date_time_hour) AS date_time_local_15min,
		hour_num,
		day_num,
		day_name,
		month_num,
		month_abbrev,
		month_name,
		day_of_week,
		week_of_year,
		day_of_year,
		is_weekday,
		is_weekend,
		is_business_hour,
		last_day_of_month
	FROM transformed.date_hour h
	CROSS JOIN cte_15min m
;
