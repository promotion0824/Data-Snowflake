-- ******************************************************************************************************************************
-- Create published view for engagement reports
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE VIEW published.event_list_viewed AS

SELECT
	anonymous_id,
	building_id,
	building_name,
	client_name,
	context_locale,
	event,
	tenant_name,
	event_text,
	id,
	original_timestamp,
	uuid_ts,
	user_id,
	received_at
FROM transformed.event_list_viewed
WHERE client_name = 'Investa'
;
