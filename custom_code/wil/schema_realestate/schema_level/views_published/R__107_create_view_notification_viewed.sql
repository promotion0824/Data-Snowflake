-- ******************************************************************************************************************************
-- Create published view for engagement reports for engagement reports
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE VIEW published.notification_viewed AS

SELECT
	anonymous_id,
	building_id,
	building_name,
	client_name,
	context_locale,
	event,
	event_text,
	id,
	original_timestamp,
	received_at,
	sent_at,
	tenant_name,
	user_id,
	uuid_ts
FROM transformed.notification_viewed
WHERE client_name = 'Investa'
;