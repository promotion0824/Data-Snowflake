-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the aggregate table
-- This is called via transformed.merge_agg_electrical_metering_daily_tk which is dependent on merge_agg_electrical_metering_hourly_tk
-- USAGE:  CALL transformed.merge_agg_site_daily_energy_scores_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************
CREATE OR REPLACE PROCEDURE transformed.merge_agg_site_daily_energy_scores_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
	EXECUTE AS CALLER
  AS
    $$
	  MERGE INTO transformed.site_daily_scores AS tgt
	  USING
	  (
			SELECT 
				site_id,
				site_name,
				date_local,
				is_weekday,
				AVG(energy_rating) * 100 AS avg_energy_rating,
				ROUND(avg_energy_rating, 8) AS energy_score,
				--AVG(energy_rating_based_on_4_weeks_ago) * 100 AS avg_energy_rating_based_on_4_weeks_ago,
				--ROUND(avg_energy_rating_based_on_4_weeks_ago, 2) AS energy_score_based_on_4_weeks_ago
				MAX(last_captured_at_local) last_captured_at_local,
				MAX(last_captured_at_utc) last_captured_at_utc,
				MAX(last_refreshed_at_utc) AS last_refreshed_at_utc
			FROM transformed.electrical_metering_detail2_v
			WHERE COALESCE(level_1_model_id,level_2_model_id,model_id_asset) IN ('dtmi:com:willowinc:Switchboard;1','dtmi:com:willowinc:Switchgear;1')
			   OR model_id_asset IN ('dtmi:com:willowinc:ElectricalMeter;1')
			GROUP BY
				site_id,
				site_name,
				date_local,
				is_weekday
 	  )
	  AS src
			  ON (    tgt.site_id = src.site_id
				  AND tgt.date = src.date_local
				 )
	  WHEN MATCHED THEN
		UPDATE 
		SET
				tgt.site_name = src.site_name,
				tgt.is_weekday = src.is_weekday,
				tgt.energy_score = src.energy_score,
				tgt._last_updated_by_task = :task_name,
				tgt.last_captured_at_local = src.last_captured_at_local,
				tgt.last_captured_at_utc = src.last_captured_at_utc,
				tgt.last_refreshed_at_utc = COALESCE(src.last_refreshed_at_utc,SYSDATE())
				
	  WHEN NOT MATCHED THEN
		INSERT (
				site_id,
				site_name,
				date,
				is_weekday,
                energy_score,
				_created_at,
				_created_by_task,
				_last_updated_by_task,
				last_captured_at_local,
				last_captured_at_utc,
				last_refreshed_at_utc
		)
		VALUES (
				src.site_id,
				src.site_name,
				src.date_local,
				is_weekday,
				src.energy_score,
				SYSDATE(), 
				:task_name,
				:task_name,
				src.last_captured_at_local,
				src.last_captured_at_utc,
				SYSDATE()
		);
    $$
;