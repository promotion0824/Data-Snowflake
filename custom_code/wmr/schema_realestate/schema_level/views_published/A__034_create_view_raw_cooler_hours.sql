CREATE OR REPLACE VIEW published.raw_cooler_hours AS
SELECT external_id, captured_at, scalar_value, enqueued_at
FROM raw.stage_Telemetry ts
WHERE scalar_value::float > 0
  AND (external_Id ilike '%cooler%hours%'
   OR external_Id ilike '%total%cost%four%'
   OR external_Id ilike '%total%throw%events%'
   OR external_Id ilike '%throw%events%four%hour%limit%'
   OR external_Id ilike '%total%cost%to%date%')
QUALIFY ROW_NUMBER() OVER (PARTITION BY ts.external_id, ts.captured_at ORDER BY enqueued_at DESC) = 1   
;