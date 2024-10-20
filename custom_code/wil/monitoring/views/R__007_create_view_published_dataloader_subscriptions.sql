-- ******************************************************************************************************************************
-- Create view
-- ******************************************************************************************************************************
CREATE OR REPLACE VIEW central_monitoring_db.published.dataloader_subscriptions AS
WITH cte_data AS (
	SELECT
		source_type,
		TRY_PARSE_JSON((sink_settings)::variant):AdlsEndpointUrl::string AS sink_endpoint,
		POSITION('dls',sink_endpoint) AS dls,
		POSITION('stg',sink_endpoint) AS stg,
		POSITION('@',entity_uniquename) AS server,
		UPPER(SUBSTR(sink_endpoint,dls-3,3)) AS environment,
		UPPER(SUBSTR(sink_endpoint,stg+3,dls-stg-6)) AS customer_identifier,
		SUBSTR(entity_uniquename,1,server-1) as entity,
		trigger_name,
		entity_is_active,
		source_is_active,
		dataset_settings,
		sink_settings,
		entity_uniquename
	FROM central_monitoring_db.transformed.dataloader_subscriptions
	WHERE source_is_active = true
	  AND entity_is_active = true
	)
SELECT 
	environment,
	customer_identifier,
	source_type,
	entity,
	trigger_name,
	entity_is_active,
	source_is_active,
	dataset_settings,
	sink_settings,
	entity_uniquename
FROM cte_data
;