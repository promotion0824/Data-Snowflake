-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.insight_occurrences AS

SELECT
		i.id AS insight_id,
		i.site_id,
		s.name AS site_name,
		t.model_id,
		i.equipment_id,
		i.type,
		i.name,
		i.description,
		i.priority,
		i.status,
		i.external_status,
		i.created_date,
		i.updated_date,
		COALESCE(i.last_occurred_date, i.detected_date) AS last_occurred_date,
		i.detected_date,
		IFF(HOUR(COALESCE(i.last_occurred_date, i.detected_date)) >= COALESCE(working_hours.default_value:hourStart,8) 
		AND HOUR(COALESCE(i.last_occurred_date, i.detected_date)) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
		d.is_weekday,
		i.source_type,
		i.source_id,
		i.rule_id,
		i.rule_name,
		i.twin_id,
		i.primary_model_id,
		i.state,
		i.occurrence_count,
        occ.id,
        occ.occurrence_id,
        occ.is_faulted,
        occ.is_valid,
        occ.started,
        occ.ended,
        DATEDIFF(hour, occ.started, occ.ended) AS duration_hours,
        occ.text,
		i._last_updated_at AS last_refreshed_at_utc
FROM transformed.insights i
	JOIN transformed.sites s
	  ON (i.site_id = s.site_id)
	JOIN transformed.dates d 
	  ON (DATE_TRUNC(day, COALESCE(i.last_occurred_date, i.detected_date)) = d.date)
    JOIN transformed.insight_occurrences occ
	  ON (i.id = occ.insight_id)
LEFT JOIN transformed.twins t 
      ON (i.equipment_id = t.unique_id)
LEFT JOIN transformed.site_defaults working_hours
      ON (i.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
     AND (working_hours._valid_from <= COALESCE(i.last_occurred_date, i.detected_date) AND working_hours._valid_to >= COALESCE(i.last_occurred_date, i.detected_date))
;