-- ******************************************************************************************************************************
-- Dimension view all dates/hours
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW transformed.dates AS
	SELECT 
		date,
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
		last_day_of_month
	FROM utils.dates d
;

