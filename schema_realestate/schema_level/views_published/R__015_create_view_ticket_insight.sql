-- ------------------------------------------------------------------------------------------------------------------------------
-- Create View
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW published.ticket_insight AS
WITH cte_tenants AS (
	SELECT 
		twin_id AS tenant_id, 
		name as tenant_name
	FROM transformed.twins
	WHERE name in (
		SELECT DISTINCT reporter_company from transformed.workflow_core_tickets
	)
	AND model_id = 'dtmi:com:willowinc:Company;1'
	AND IFNULL(is_deleted,FALSE) = FALSE
)
SELECT
	t.id,
	t.customer_id,
	s.building_id,
	s.building_name,
	t.site_id,
	s.name AS site_name, 
	t.floor_code,
	regexp_substr(t.floor_code, '[[:alpha:]]+') AS level_alpha,
	NULLIF(REGEXP_REPLACE(t.floor_code, '[a-z/-/A-z/./#/*]', ''),'') AS level_numeric,
	level_alpha || IFNULL((LPAD(level_numeric,2,0)),'') AS level_number,
	sc_floors.sort_order AS floor_sort_order,
	t.sequence_number,
	t.priority,
	t.status,
	t.status_description,
	t.issue_type,
	t.issue_id,
	t.issue_name,
	t.description,
	t.cause,
	t.solution,
	t.reporter_id,
	t.reporter_name,
	t.reporter_phone,
	t.reporter_email,
	tnt.tenant_id AS reporter_company_id,
	t.reporter_company,
	t.assignee_id,
	t.assignee_name,
	t.due_date,
	COALESCE(external_created_date,created_date) AS created_date,
	IFF(HOUR(created_date) >= COALESCE(working_hours.default_value:hourStart,8) 
	AND HOUR(created_date) <  COALESCE(working_hours.default_value:hourEnd, 18), true, false) AS is_working_hour,
	d.is_weekday,
	t.created_date AS created_date_dbcore,
	t.up_dated_date AS updated_date_dbcore,
	t.external_created_date,
	t.external_updated_date,
	t.last_updated_by_external_source,
	t.resolved_date,
	t.closed_date,
	t.category_id,
	t.category,
	t.source_type,
	t.source_id,
	t.external_id,
	t.external_status,
	t.external_metadata,
	t.summary,
	t.assignee_type,
	t.insight_id,
	t.insight_name,
	t.latitude,
	t.longitude,
	t.creator_id,
	t.occurrence,
	t.scheduled_date,
	t.notes,
	t.is_template,
	t.template_id,
	t.recurrence,
	t.overdue_threshold,
	t.assets,
	t.tasks,
	t.attachments,
	t.data_value,
	CONVERT_TIMEZONE( 'UTC', s.time_zone, MAX(t._last_updated_at) OVER ()) AS _last_updated_at
FROM transformed.workflow_core_tickets t
	JOIN transformed.sites s
	  ON (t.site_id = s.site_id)
	JOIN transformed.dates d 
	  ON (DATE_TRUNC(day, t.created_date) = d.date)
	LEFT JOIN cte_tenants tnt
	  ON (t.reporter_company = tnt.tenant_name)
	LEFT JOIN transformed.site_core_floors sc_floors
	  ON (t.floor_code = sc_floors.floor_code)
	 AND (t.site_id = sc_floors.site_id)
	LEFT JOIN transformed.site_defaults working_hours
	  ON (t.site_id = working_hours.site_id AND working_hours.type = 'WorkingHours')
	 AND (working_hours._valid_from <= created_date AND working_hours._valid_to >= created_date)
;