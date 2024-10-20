-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE SECURE VIEW published.insights AS
WITH cte_lucernix AS (
	SELECT site_id, building_id, building_name, custom_properties:externalIds.Real_Estate_ID::STRING as LucernixId
	FROM transformed.buildings
)
,cte_impact_pivot AS (
    SELECT *
    FROM (SELECT DISTINCT insight_id,name,value FROM transformed.impact_scores)
    PIVOT(MIN(value) FOR name IN ('Daily Avoidable Cost', 'Daily Avoidable Energy', 'Total Cost to Date', 'Total Energy to Date', 'Priority')) 
     AS P(insight_id, daily_avoidable_cost,daily_avoidable_energy,total_cost_to_date,total_energy_to_date,priority)
)
SELECT
		i.id,
		l.LucernixId AS lucernix_id,
		l.building_id,
		l.building_name,
		i.site_id,
		s.name AS site_name,
		i.sequence_number,
		floors.level_name AS floor_name,
		floors.level_number,
		floors.level_code AS floor_code,
		floors.floor_sort_order,
		t.model_id,
		i.equipment_id,
		i.type,
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
		i.recommendation,
		i.external_id,
		i.external_metadata,
		i.state,
		i.occurrence_count,
		i._last_updated_at AS last_refreshed_at_utc
FROM transformed.insights i
	JOIN transformed.sites s
	  ON (i.site_id = s.site_id)
	JOIN cte_lucernix l
	  ON (i.site_id = l.site_id)
	JOIN transformed.dates d 
	  ON (DATE_TRUNC(day, COALESCE(i.last_occurred_date, i.detected_date)) = d.date)
LEFT JOIN transformed.twins t 
      ON (i.equipment_id = t.unique_id)
LEFT JOIN transformed.levels_buildings floors
	  ON (t.floor_id = floors.floor_id)
LEFT JOIN cte_impact_pivot impact
	  ON (i.id = impact.insight_id)
LEFT JOIN transformed.site_defaults working_hours
      ON (i.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
     AND (working_hours._valid_from <= COALESCE(i.last_occurred_date, i.detected_date) AND working_hours._valid_to >= COALESCE(i.last_occurred_date, i.detected_date))
--; GRANT SELECT ON VIEW published.insights TO SHARE external_share
;

