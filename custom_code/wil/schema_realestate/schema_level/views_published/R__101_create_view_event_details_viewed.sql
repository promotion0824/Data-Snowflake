-- ******************************************************************************************************************************
-- Create published view for engagement reports
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE VIEW published.event_details_viewed AS

SELECT
	anonymous_id,
	building_id,
	building_name,
	client_name,
	context_locale,
	event,
	event_category,
	event_text,
	id,
	original_timestamp,
	received_at,
	sent_at,
	tenant_name,
	user_id,
	uuid_ts
FROM transformed.event_details_viewed
WHERE client_name = 'Investa'
;