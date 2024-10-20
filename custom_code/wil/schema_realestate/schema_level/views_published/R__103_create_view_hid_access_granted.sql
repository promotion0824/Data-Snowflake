-- ******************************************************************************************************************************
-- Create published view for engagement reports
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE VIEW published.hid_access_granted AS

SELECT
	anonymous_id,
	building_id,
	building_name,
	client_name,
	event,
	event_text,
	id,
	original_timestamp,
	received_at,
	sent_at,
	tenant_name,
	user_id,
	uuid_ts
FROM transformed.hid_access_granted
WHERE client_name = 'Investa'
;