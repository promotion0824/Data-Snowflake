-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates the aggregate table
-- This is called via transformed.merge_agg_electrical_demand_hourly_tk which is scheduled by CRON
-- USAGE:  CALL transformed.merge_agg_electrical_demand_hourly_sp(SYSTEM$CURRENT_USER_TASK_NAME());
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.merge_agg_electrical_demand_hourly_sp(task_name VARCHAR)
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
	  MERGE INTO transformed.agg_electrical_demand_hourly AS tgt
		USING ( 
			WITH watermark AS 
				(
				  SELECT
					IFNULL(MAX(date_local),'2019-01-01') AS max_date,
					site_id
				  FROM transformed.agg_electrical_demand_hourly
				  WHERE date_local <= DATEADD('d',-1,SYSDATE())
				  GROUP BY site_id
				)
            ,cte_daily_peak AS (
                WITH cte_hourly_power AS (
                SELECT
                    ts.site_id,
                    date_local,
                    date_time_local_hour,
                    SUM(CASE lower(ts.unit) 
                          WHEN 'w'  THEN ts.avg_value_hour / 1000.0 
                          WHEN 'kw' THEN ts.avg_value_hour
                        ELSE NULL
                    END) AS building_hourly_power,
                    MAX(building_hourly_power) OVER (PARTITION BY ts.site_id, date_local) AS daily_peak_demand_building
                FROM transformed.agg_electrical_metering_hourly ts
                LEFT JOIN watermark
                  ON (ts.site_id = watermark.site_id)
                WHERE date_time_local_hour > DATEADD('d',-1,COALESCE(max_date,'2018-01-01'))
                    AND sensor_type = 'Power'
                    GROUP BY ts.site_id,date_local,date_time_local_hour
                    ORDER BY date_time_local_hour
            ) 
            SELECT DISTINCT 
                hp.site_id,
                hp.daily_peak_demand_building,
                max_p.date_time_local_hour AS building_peak_hour
            FROM cte_hourly_power hp
            JOIN cte_hourly_power max_p 
              ON hp.site_id = max_p.site_id 
             AND hp.daily_peak_demand_building = max_p.building_hourly_power
             AND hp.date_local = max_p.date_local
            )
            ,cte_agg_hourly_meter AS (
                    SELECT
                        hourly.site_id,
                        ca.asset_id, 
                        hourly.date_local,
                        hourly.date_time_local_hour,
                        SUM(CASE lower(hourly.unit) 
                              WHEN 'w'  THEN avg_value_hour / 1000.0 
                              WHEN 'kw' THEN avg_value_hour
                            ELSE NULL
                        END) AS hourly_power_consumption,
                        SUM(values_count) AS values_count,
                        :task_name,
                        MAX(last_captured_at_local) AS last_captured_at_local,
                        MAX(last_captured_at_utc) AS last_captured_at_utc,
                        MAX(last_refreshed_at_utc) AS last_refreshed_at_utc,
                        CONVERT_TIMEZONE( 'UTC',MAX(ca.time_zone), SYSDATE()) AS last_refreshed_at_local,
                        MAX(last_enqueued_at_utc) AS last_enqueued_at_utc
                    FROM transformed.agg_electrical_metering_hourly hourly
                    JOIN transformed.capabilities_assets ca
                      ON (hourly.trend_id = ca.trend_id)
                    LEFT JOIN watermark
                      ON (hourly.site_id = watermark.site_id)
                    WHERE date_time_local_hour > DATEADD('d',-1,COALESCE(max_date,'2018-01-01'))
                      AND sensor_type = 'Power'
                    GROUP BY 
                        hourly.site_id,
                        ca.asset_id,
                        hourly.date_local,
                        hourly.date_time_local_hour
                    )
            SELECT 
                hourly.site_id,
                asset_id,
                hourly.date_local,
                hourly.date_time_local_hour,
                MAX(dp.daily_peak_demand_building) OVER (PARTITION BY hourly.site_id, hourly.date_local) AS daily_peak_demand_building,
                CASE WHEN dp.building_peak_hour IS NOT NULL THEN true ELSE false END as is_peak_hour,
                MAX(dp.building_peak_hour) OVER (PARTITION BY hourly.site_id, hourly.date_local) AS building_peak_hour,
                hourly_power_consumption,
                values_count,
                :task_name,
                last_captured_at_local,
                last_captured_at_utc,
                last_refreshed_at_utc,
                last_refreshed_at_local,
                last_enqueued_at_utc
            FROM cte_agg_hourly_meter hourly
            LEFT JOIN cte_daily_peak dp
                ON hourly.site_id = dp.site_id
                AND hourly.date_time_local_hour = dp.building_peak_hour
            )
                AS src
                        ON (    
                                tgt.asset_id = src.asset_id
                            AND tgt.date_time_local_hour = src.date_time_local_hour
                            )
                WHEN MATCHED THEN
                    UPDATE 
                    SET
                            tgt.daily_peak_demand_building = src.daily_peak_demand_building,
                            tgt.is_peak_hour = src.is_peak_hour,
                            tgt.building_peak_hour = src.building_peak_hour,
                            tgt.hourly_power_consumption = src.hourly_power_consumption,
                            tgt.values_count = src.values_count,
                            tgt._last_updated_by_task = :task_name,
                            tgt.last_captured_at_local = src.last_captured_at_local,
                            tgt.last_captured_at_utc = src.last_captured_at_utc,
                            tgt.last_refreshed_at_utc = SYSDATE(),
                            tgt.last_refreshed_at_local = src.last_refreshed_at_local,
                            tgt.last_enqueued_at_utc = src.last_enqueued_at_utc
                WHEN NOT MATCHED THEN
                    INSERT (
                            site_id,
                            asset_id,
                            date_local,
                            date_time_local_hour,
                            daily_peak_demand_building,
                            is_peak_hour,
                            building_peak_hour,
                            hourly_power_consumption,
                            values_count,
                            _created_at,
                            _created_by_task,
                            _last_updated_by_task,
                            last_captured_at_local,
                            last_captured_at_utc,
                            last_refreshed_at_utc,
                            last_refreshed_at_local,
                            last_enqueued_at_utc
                    )
                    VALUES (
                            src.site_id,
                            src.asset_id,
                            src.date_local,
                            src.date_time_local_hour,
                            src.daily_peak_demand_building,
                            src.is_peak_hour,
                            src.building_peak_hour,
                            src.hourly_power_consumption,
                            src.values_count, 
                            SYSDATE(),
                            :task_name,
                            :task_name,
                            last_captured_at_local,
                            last_captured_at_utc,
                            SYSDATE(),
                            last_refreshed_at_local,
                            last_enqueued_at_utc
                    );
                $$
            ;