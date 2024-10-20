-- ******************************************************************************************************************************
-- Create view Time Series Enriched in published schema
--
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.time_series_enriched AS
	SELECT *, enqueued_at AS enqueued_at_utc
	FROM transformed.time_series_enriched
;