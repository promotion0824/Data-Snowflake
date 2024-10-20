CREATE OR REPLACE VIEW published.insight_occurrences AS

SELECT
		i.id AS insight_id,
		i.site_name,
		i.model_id,
		i.equipment_id,
		i.type,
		i.name,
		i.priority,
		i.status,
		i.created_date,
		i.updated_date,
		i.last_occurred_date,
		i.detected_date,
		i.total_throw_away_events,
		i.total_throw_away_events_four_hour_limit,
		i.daily_avoidable_cost,
		i.total_cost_to_date,
		i.impact_priority,
		i.allowable_time_above_temperature_target,
		i.cooler_hours,
		i.emergency_maintenance_risk,
		i.has_spoiled,
		i.lost_margin_risk,
		i.stock_throw_risk,
		i.store_associate_labor_risk,
		i.time_out_of_target,
		i.time_till_restocked,
		i.time_till_spoil,
		i.total_risk,
		i.source_type,
		i.rule_name,
		i.twin_id,
		i.state,
		i.occurrence_count,
		occ.id,
		occ.occurrence_id,
		occ.is_faulted,
		occ.is_valid,
		occ.started,
		occ.ended,
		DATEDIFF(hour, occ.started, occ.ended) AS duration_hours
FROM published.insights i
    JOIN transformed.insight_occurrences occ
	  ON (i.id = occ.insight_id)
WHERE
        occ.is_faulted = true
  AND   occ.is_valid = true
  --AND occ.started <= i.last_occured_date
;
