-- *******************************************************************************************************************************************************************
-- Create view for baseline energy_baseline
-- To import the csv, use this sproc:  CALL utils.create_table_from_stage('@wil_automation_csv_esg/Compliance/Validation', 'data_compliance.raw_validation');
-- *******************************************************************************************************************************************************************

CREATE OR REPLACE VIEW published.energy_baseline AS 
SELECT DISTINCT
	s.site_id,
	b."Site" AS site_name,
	b."State" AS state,
	LEFT(month,4)::INTEGER AS year,
	TO_CHAR(TO_DATE(replace(month,'_','-') || '-01'),'MMMM') AS month,
	energy_consumption_mWH::DECIMAL(12,2) AS energy_consumption_mWH
FROM raw.energy_baseline b
unpivot(energy_consumption_mWH FOR month IN ("2020_01", "2020_02", "2020_03", "2020_04", "2020_05", "2020_06", "2020_07", "2020_08", "2020_09", "2020_10", "2020_11", "2020_12")
       )
LEFT JOIN transformed.sites s ON b."Site" = s.name
;