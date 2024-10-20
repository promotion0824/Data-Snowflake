-- ------------------------------------------------------------------------------------------------------------------------------
-- Create rolling 7 day view for performance engineering
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW transformed.telemetry_hostedby_twins_rolling_6_months AS
    WITH cte_hostedby AS (
	SELECT 
    	ts.trend_id,
    	ts.site_id,
        s.name AS site_name,
    	tt.twin_id AS asset_id,
        tt.name AS asset_name,
        ts.name AS capability_name,
        ts.model_id AS model_id_capability,
        tt.model_id AS model_id_asset,
        r.relationship_name
	FROM transformed.twins ts
	JOIN transformed.twins_relationships_deduped r 
		   ON (ts.twin_id = r.source_twin_id)
	JOIN transformed.twins tt 
		   ON (tt.twin_id = r.target_twin_id)
	LEFT JOIN transformed.ontology_buildings os 
		   ON (ts.model_id = os.id)
	LEFT JOIN transformed.sites s 
		   ON (s.site_id = ts.site_id)
	WHERE 
        r.relationship_name IN ('hostedBy') 
		AND IFNULL(r.is_deleted,FALSE) = FALSE
		AND IFNULL(ts.is_deleted,FALSE) = FALSE
		AND IFNULL(tt.is_deleted,FALSE) = FALSE
    )
    SELECT 
        ts.date_local,
        ts.timestamp_local,
        ts.timestamp_utc,
        ts.trend_id,
        ts.site_id,
        ts.telemetry_value,
        ts.date_time_local_15min,
        ts.date_time_local_hour,
        ca.asset_id,
        ca.asset_name,
        ca.capability_name,
        ca.model_id_capability,
        ca.model_id_asset
FROM cte_hostedby AS ca
JOIN transformed.time_series_enriched TS 
  ON (ts.trend_id = ca.trend_id)
WHERE ts.date_local >= DATEADD(MONTH,-6,CURRENT_DATE)
;