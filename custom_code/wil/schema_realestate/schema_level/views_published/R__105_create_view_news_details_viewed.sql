-- ******************************************************************************************************************************
-- Create published view for engagement reports
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE VIEW published.news_details_viewed AS

SELECT
	anonymous_id,
	article_title,
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
FROM transformed.news_details_viewed
WHERE client_name = 'Investa'
;