-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.insights AS
WITH cte_impact_pivot AS (
    SELECT *
    FROM (SELECT DISTINCT insight_id,name,value FROM transformed.impact_scores)
    PIVOT(MIN(value) FOR name IN ('Daily Avoidable Cost', 'Daily Avoidable Energy', 'Total Cost to Date', 'Total Energy to Date', 'Priority')) 
     AS P(insight_id, daily_avoidable_cost,daily_avoidable_energy,total_cost_to_date,total_energy_to_date,priority)
)
SELECT
		i.id,
		i.customer_id,
		s.building_id,
		s.building_name,
		i.site_id,
		s.name AS site_name,
		i.sequence_number,
		floors.level_name AS floor_name,
		floors.level_number,
		floors.level_code AS floor_code,
		floors.floor_sort_order,
		t.model_id,
		i.equipment_id,
		it.insight_type,
		i.name,
		i.description,
		i.priority,
		i.status,
		i.external_status,
		impact.daily_avoidable_cost,
		impact.daily_avoidable_energy,
		impact.total_cost_to_date,
		impact.total_energy_to_date,
		impact.priority AS impact_priority,
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
		t2.twin_id AS room_id,
		t2.name AS room,
		i.recommendation,
		i.external_id,
		i.external_metadata,
		i.state,
		i.occurrence_count,
		i._last_updated_at AS last_refreshed_at_utc
FROM transformed.insights i
	JOIN transformed.dates d 
	  ON (DATE_TRUNC('DAY', COALESCE(i.last_occurred_date, i.detected_date)) = d.date)
LEFT JOIN transformed.insight_types it
	  ON (i.type = it.insight_type_id)
LEFT JOIN transformed.sites s
	  ON (i.site_id = s.site_id)
LEFT JOIN transformed.twins t 
      ON (i.equipment_id = t.unique_id)
LEFT JOIN transformed.levels_buildings floors
	  ON (t.floor_id = floors.floor_id)
LEFT JOIN transformed.twins_relationships_deduped tr 
	  ON (t.twin_id = tr.source_twin_id)
	 AND (tr.relationship_name = 'locatedIn')
LEFT JOIN transformed.twins t2 
	  ON tr.target_twin_id = t2.twin_id 
	AND (t2.model_id = 'dtmi:com:willowinc:Room;1')
LEFT JOIN cte_impact_pivot impact
	  ON (i.id = impact.insight_id)
LEFT JOIN transformed.site_defaults working_hours
      ON (i.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
     AND (working_hours._valid_from <= COALESCE(i.last_occurred_date, i.detected_date) AND working_hours._valid_to >= COALESCE(i.last_occurred_date, i.detected_date))

WHERE 
    IFNULL( t.is_deleted,false) = false
AND IFNULL(tr.is_deleted,false) = false
AND IFNULL(t2.is_deleted,false) = false
QUALIFY ROW_NUMBER() OVER (PARTITION BY i.id ORDER BY t2.twin_id NULLS LAST, i._last_updated_at DESC) = 1
;