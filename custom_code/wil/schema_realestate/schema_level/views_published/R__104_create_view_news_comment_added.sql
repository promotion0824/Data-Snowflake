-- ******************************************************************************************************************************
-- Create published view for engagement reports
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE VIEW published.news_comment_added AS

SELECT 
	anonymous_id,
	building_id,
	building_name,
	client_name,
	context_locale,
	event,
	event_text,
	id,
	news_category,
	original_timestamp,
	received_at,
	sent_at,
	tenant_name,
	user_id,
	uuid_ts
FROM transformed.news_comment_added
WHERE client_name = 'Investa'
;