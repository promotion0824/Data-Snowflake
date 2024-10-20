-- ******************************************************************************************************************************
-- Create tables for engagement
-- ******************************************************************************************************************************

CREATE OR REPLACE VIEW published.engagement_event_details_viewed AS 
SELECT 
	anonymous_id,
	t.building_id,
	t.building_name,
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
	uuid_ts,
	s.site_name
FROM raw.event_details_viewed t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;

CREATE OR REPLACE VIEW published.engagement_event_list_viewed AS 
SELECT 
	anonymous_id,
	t.building_id,
	t.building_name,
	client_name,
	context_locale,
	event,
	tenant_name,
	event_text,
	id,
	original_timestamp,
	uuid_ts,
	user_id,
	received_at,
	s.site_name
FROM raw.event_list_viewed t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;

CREATE OR REPLACE VIEW published.engagement_news_comment_added AS 
SELECT 
	anonymous_id,
	t.building_id,
	t.building_name,
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
	uuid_ts,
	s.site_name
FROM raw.news_comment_added t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;

CREATE OR REPLACE VIEW published.engagement_news_details_viewed AS 
SELECT 
	anonymous_id,
	article_title,
	t.building_id,
	t.building_name,
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
	uuid_ts,
	s.site_name
FROM raw.news_details_viewed t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;

CREATE OR REPLACE VIEW published.engagement_news_list_viewed AS 
SELECT 
	anonymous_id,
	t.building_id,
	t.building_name,
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
	uuid_ts,
	s.site_name
FROM raw.news_list_viewed t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;

CREATE OR REPLACE VIEW published.engagement_user_session_active AS 
SELECT 
	anonymous_id,
	t.building_id,
	t.building_name,
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
	uuid_ts,
	s.site_name
FROM raw.user_session_active t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;

CREATE OR REPLACE VIEW published.engagement_hid_access_granted AS 
SELECT 
	anonymous_id,
	t.building_id,
	t.building_name,
	client_name,
	event,
	event_text,
	id,
	original_timestamp,
	received_at,	
	sent_at,	
	tenant_name,
	user_id,
	uuid_ts,
	s.site_name
FROM raw.hid_access_granted t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;

CREATE OR REPLACE VIEW published.engagement_notification_viewed AS 
SELECT 
	anonymous_id,
	t.building_id,
	t.building_name,
	client_name,
	context_locale,
	event,
	EVENT_TEXT,
	id,
	original_timestamp,
	received_at,
	sent_at,	
	tenant_name,
	user_id,
	uuid_ts,
	s.site_name
FROM raw.notification_viewed t
LEFT JOIN transformed.buildings s on t.building_name = s.building_name;