-- ******************************************************************************************************************************
-- Stored procedure to persist the tenant engagement data
-- ******************************************************************************************************************************
USE DATABASE segment_events_db
;
CREATE OR REPLACE PROCEDURE transformed.insert_notification_viewed_sp()
    RETURNS STRING
    LANGUAGE SQL
  AS
    $$
      BEGIN
			TRUNCATE TABLE transformed.notification_viewed
			;
			INSERT INTO transformed.notification_viewed ( 
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
			FROM willow_experience_android.notification_viewed
			;
			INSERT INTO transformed.notification_viewed ( 
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
			FROM willow_experience_ios.notification_viewed
			;
      END;
    $$
;
