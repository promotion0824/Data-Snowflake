-- ******************************************************************************************************************************
-- Stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE PROCEDURE transformed.insert_news_details_viewed_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
			TRUNCATE TABLE  transformed.news_details_viewed
			;
			INSERT INTO transformed.news_details_viewed ( 
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
			)  
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
			FROM willow_experience_android.news_details_viewed
			;
			INSERT INTO transformed.news_details_viewed ( 
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
			)  
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
			FROM willow_experience_ios.news_details_viewed
			;
			INSERT INTO transformed.news_details_viewed ( 
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
			)  
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
			FROM willow_experience_website.news_details_viewed
			;
      END;
    $$
;
