-- ******************************************************************************************************************************
-- Dimension view all dates/hours
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW published.dates AS
	SELECT *
	FROM transformed.dates
;
