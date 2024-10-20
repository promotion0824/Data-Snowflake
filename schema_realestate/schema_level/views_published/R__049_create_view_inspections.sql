-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.inspections AS

SELECT
        id,
        site_id,
        name,
        floor_code,
        zone_id,
        asset_id,
        assigned_workgroup_id,
        frequency_in_hours,
        start_date,
        end_date,
        last_record_id,
        is_archived,
        sort_order,
        frequency,
        frequency_unit,
        twin_id,
        raw_json_value,
        _created_at,
        _last_updated_at,
        _stage_record_id,
        _loader_run_id,
        _ingested_at,
        _staged_at
FROM transformed.inspections
;