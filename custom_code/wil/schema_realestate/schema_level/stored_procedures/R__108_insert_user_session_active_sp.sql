-- ******************************************************************************************************************************
-- Stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE PROCEDURE transformed.insert_user_session_active_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
			TRUNCATE TABLE transformed.user_session_active
			;
			INSERT INTO transformed.user_session_active ( 
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
			FROM willow_experience_android.user_session_active
			;
			INSERT INTO transformed.user_session_active ( 
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
			FROM willow_experience_ios.user_session_active
			;
			INSERT INTO transformed.user_session_active ( 
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
			FROM willow_experience_website.user_session_active
			;
      END;
    $$
;