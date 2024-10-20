-- ******************************************************************************************************************************
-- Stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE PROCEDURE transformed.insert_news_list_viewed_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
			TRUNCATE TABLE transformed.news_list_viewed
			;
			INSERT INTO transformed.news_list_viewed (
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
			)
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
			FROM willow_experience_android.news_list_viewed
			;
			INSERT INTO transformed.news_list_viewed (
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
			)
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
			FROM willow_experience_ios.news_list_viewed
			;
			INSERT INTO transformed.news_list_viewed (
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
			)
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
			FROM willow_experience_website.news_list_viewed
			;
      END;
    $$
;
