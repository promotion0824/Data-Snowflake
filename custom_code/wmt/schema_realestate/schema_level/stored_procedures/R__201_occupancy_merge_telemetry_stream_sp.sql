-- ******************************************************************************************************************************
-- Stored procedure that incrementally populates table
-- ******************************************************************************************************************************

CREATE OR REPLACE PROCEDURE transformed.occupancy_merge_telemetry_stream_sp(task_name VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
  AS
  $$
    BEGIN
      
        CREATE OR REPLACE TRANSIENT TABLE transformed.occupancy_transient_telemetry AS
        SELECT 
            ts.trend_id,
            ts.external_id,
            ts.timestamp_utc,
            ts.timestamp_local,
            ts.date_local,
            ts.telemetry_value,
            ts.enqueued_at
        FROM transformed.telemetry_str_occupancy ts
        WHERE EXISTS (SELECT 1 FROM transformed.occupancy_space_twins t WHERE ts.trend_id = t.trend_id)
        UNION ALL
        SELECT 
            ts.trend_id,
            ts.external_id,
            ts.timestamp_utc,
            ts.timestamp_local,
            ts.date_local,
            ts.telemetry_value,
            ts.enqueued_at
        FROM transformed.telemetry_str_occupancy ts
        WHERE EXISTS (SELECT 1 FROM transformed.occupancy_space_twins t WHERE ts.external_id = t.external_id AND ts.trend_id IS NULL)
        ;

        -- If the first of new records has the same value as the last of the existing records set valid_to to valid_to of the new record 
        UPDATE transformed.occupancy_space_telemetry AS tgt
        SET 
          tgt.valid_to = CASE WHEN tgt.telemetry_value = src.telemetry_value THEN src.timestamp_local ELSE DATEADD(ms, -1, src.timestamp_local) END
        FROM (
          SELECT 
            trend_id,
            external_id,
            timestamp_local,
            telemetry_value
          FROM transformed.occupancy_transient_telemetry
          QUALIFY ROW_NUMBER() OVER (PARTITION BY trend_id,external_id ORDER BY timestamp_local) = 1
        ) AS src
        WHERE 
              IFNULL(tgt.external_id,'')  = IFNULL(src.external_id,'') 
          AND IFNULL(tgt.trend_id,'') = IFNULL(src.trend_id,'')
          AND src.timestamp_local > tgt.valid_from
          AND tgt.valid_to IS NULL
          ;

        -- Insert new records
        INSERT INTO transformed.occupancy_space_telemetry
        (date_local, external_id, trend_id, telemetry_value, timestamp_utc, valid_from, valid_to, enqueued_at)
        SELECT
            date_local,
            external_id,
            trend_id,
            telemetry_value,
            timestamp_utc,
            timestamp_local AS valid_from, 
            LEAD(timestamp_local) OVER (PARTITION BY trend_id ORDER BY timestamp_local)  AS valid_to,
            enqueued_at
        FROM transformed.occupancy_transient_telemetry ts
        WHERE enqueued_at > (SELECT IFNULL(MAX(enqueued_at),'2023-01-01') FROM transformed.occupancy_space_telemetry)
          AND NOT EXISTS (SELECT 1 FROM transformed.occupancy_space_telemetry sp 
                          WHERE (ts.trend_id = sp.trend_id OR ts.external_id = sp.external_id) 
                            AND  ts.timestamp_local = sp.valid_from)
        ;
      END;
  $$
  ;
