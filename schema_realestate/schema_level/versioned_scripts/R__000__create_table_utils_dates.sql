-- ------------------------------------------------------------------------------------------------------------------------------
-- Simple date dimension
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE utils.dates AS 
  WITH cte_date AS (

    SELECT DATEADD(DAY, SEQ4(), '2020-01-01') AS my_date
    FROM TABLE(GENERATOR(ROWCOUNT=>4000))

  )
  SELECT
    TO_DATE(my_date) AS date,
    DAY(my_date) AS day,
    MONTH(my_date) AS month,
    YEAR(my_date) AS year,
    QUARTER(date) AS quarter,
    DAYNAME(my_date) AS day_name_short,
    ['Sunday', 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][DAYOFWEEK(my_date) ]::VARCHAR as day_name,
    MONTHNAME(my_date) AS month_name,
    DAYOFWEEKISO(my_date) AS day_of_week,
    WEEKOFYEAR(my_date) AS week_of_year,
    DAYOFYEAR(my_date) AS day_of_year,
    CASE WHEN day_of_week > 5 THEN false ELSE true END AS is_weekday,
    LAST_DAY(DATE, 'MONTH') AS last_day_of_month
  FROM cte_date;
