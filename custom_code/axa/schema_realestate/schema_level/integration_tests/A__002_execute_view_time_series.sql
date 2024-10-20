-- ******************************************************************************************************************************
-- SELECT Top 1 from time_series view(s)
-- add a date filter for quicker execution
-- ******************************************************************************************************************************

SELECT TOP 1 * FROM PUBLISHED.OCCUPANCY_DOORS_15_MINUTE WHERE date_local >= DATEADD('d',-1,current_timestamp);
SELECT TOP 1 * FROM PUBLISHED.TIMESERIES_PEOPLECOUNT WHERE date_local >= DATEADD('d',-1,current_timestamp);
