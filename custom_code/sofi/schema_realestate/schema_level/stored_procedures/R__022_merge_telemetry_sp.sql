-- ******************************************************************************************************************************
-- Stored procedure that merges from raw to transformed
-- USAGE:  CALL transformed.merge_telemetry_sp();
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE raw.merge_telemetry_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
		   MERGE INTO transformed.telemetry AS tgt 
		   USING (
			SELECT
				CONVERT_TIMEZONE( 'UTC', COALESCE(s.time_zone,'America/Los_Angeles'), ts.captured_at) AS timestamp_local,
                TO_DATE(SPLIT_PART(TIME_SLICE(timestamp_local, 15, 'MINUTE'),' ',1) ) AS date_local,
				ts.captured_at AS timestamp_utc,
				t.site_id,
				ts.trend_id,
				COALESCE(ts.external_id, t.external_id) AS external_id,
				CASE WHEN IS_REAL(scalar_value) = true THEN TO_DOUBLE(scalar_value) ELSE nulL END AS telemetry_value,
				ts.connector_id,
                COALESCE(ts.dt_id, t.twin_id) AS dt_id,
				ts.enqueued_at,
				ts.latitude,
				ts.longitude,
				ts.altitude,
				ts.properties,
				ts.exported_time,
				ts._ingested_at,
				ts.stage_file_name
			FROM raw.stage_telemetry_str ts
			JOIN (SELECT trend_id,site_id,external_id,twin_id
					FROM transformed.twins 
					WHERE trend_id IS NOT NULL
					AND IFNULL(is_deleted,false) = false
					QUALIFY ROW_NUMBER() OVER (PARTITION BY twin_id, trend_id ORDER BY IFNULL(is_deleted,false), _ingested_at DESC) = 1
				 ) t ON ts.trend_id = t.trend_id AND ts.trend_id IS NOT NULL
			LEFT JOIN transformed.directory_core_sites s 
				ON (s.site_id = t.site_id )
			QUALIFY ROW_NUMBER() OVER (PARTITION BY ts.trend_id, ts.captured_at ORDER BY enqueued_at DESC) = 1
               
           UNION ALL
               
			SELECT
				CONVERT_TIMEZONE( 'UTC',COALESCE(s.time_zone,'America/Los_Angeles'), ts.captured_at) AS timestamp_local,
                TO_DATE(SPLIT_PART(TIME_SLICE(timestamp_local, 15, 'MINUTE'),' ',1) ) AS date_local,
				ts.captured_at AS timestamp_utc,
				t.site_id,
				t.trend_id,
				ts.external_id,
				CASE WHEN IS_REAL(scalar_value) = true THEN TO_DOUBLE(scalar_value) ELSE nulL END AS telemetry_value,
				ts.connector_id,
                COALESCE(ts.dt_id, t.twin_id) AS dt_id,
				ts.enqueued_at,
				ts.latitude,
				ts.longitude,
				ts.altitude,
				ts.properties,
				ts.exported_time,
				ts._ingested_at,
				ts.stage_file_name
			FROM raw.stage_telemetry_str ts
			JOIN (SELECT trend_id,site_id,external_id,twin_id
						FROM transformed.twins 
						WHERE external_id IS NOT NULL
					    AND IFNULL(is_deleted,false) = false
						QUALIFY ROW_NUMBER() OVER (PARTITION BY twin_id, external_id ORDER BY IFNULL(is_deleted,false),site_id,_ingested_at DESC) = 1
             ) t ON ts.external_id = t.external_id 
			LEFT JOIN transformed.directory_core_sites s 
				ON (t.site_id = s.site_id)
            WHERE ts.trend_id IS NULL AND ts.external_id IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ts.external_id, ts.captured_at ORDER BY enqueued_at DESC) = 1         
		  ) AS src
			 ON (tgt.date_local = src.date_local)
			AND (tgt.timestamp_local = src.timestamp_local)
			AND (tgt.timestamp_utc = src.timestamp_utc)
			AND (tgt.external_id = src.external_id) 
			AND (IFNULL(tgt.trend_id,'') = IFNULL(src.trend_id,'')) 
		  WHEN MATCHED THEN
			UPDATE 
			SET 
			  tgt.date_local = src.date_local,
			  tgt.timestamp_local = src.timestamp_local,
			  tgt.timestamp_utc = src.timestamp_utc,
			  tgt.site_id = src.site_id,
			  tgt.trend_id = src.trend_id,
			  tgt.external_id = src.external_id,
			  tgt.telemetry_value = src.telemetry_value,
			  tgt.connector_id = src.connector_id,
			  tgt.dt_id = src.dt_id,
			  tgt.enqueued_at = src.enqueued_at,
			  tgt.latitude = src.latitude,
			  tgt.longitude = src.longitude,
			  tgt.altitude = src.altitude,
			  tgt.properties = src.properties,
			  tgt.exported_time = src.exported_time,
			  tgt._last_updated_at = SYSDATE(),
			  tgt.stage_file_name = src.stage_file_name
		  WHEN NOT MATCHED THEN
			INSERT (
				 date_local,
				 timestamp_local,
				 timestamp_utc,
				 site_id,
				 trend_id,
				 external_id,
				 telemetry_value,
				 connector_id,
				 dt_id,
				 enqueued_at,
				 latitude,
				 longitude,
				 altitude,
				 properties,
				 exported_time,
				 _created_at,
				 _last_updated_at,
				 stage_file_name
				)		
			VALUES (
				 src.date_local,
				 src.timestamp_local,
				 src.timestamp_utc,
				 src.site_id,
				 src.trend_id,
				 src.external_id,
				 src.telemetry_value,
				 src.connector_id,
				 src.dt_id,
				 src.enqueued_at,
				 src.latitude,
				 src.longitude,
				 src.altitude,
				 src.properties,
				 src.exported_time,
				 src._ingested_at,
				 SYSDATE(),
				 stage_file_name
			);
		  EXCEPTION
          WHEN statement_error THEN
            RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT_ERROR',
                                    'SQLCODE', sqlcode,
                                    'SQLERRM', sqlerrm,
                                    'SQLSTATE', sqlstate);
      END;		
    $$