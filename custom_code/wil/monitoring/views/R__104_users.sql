CREATE OR REPLACE VIEW central_monitoring_db.published.users AS

SELECT 
    u.customer,
    u.email,
    u.id,
    u.first_name,
    u.last_name,
    u.status,
    u.group_id,
    u.group_name,
    u.group_type_id,
    u.group_type,
    u.role_name,
    u.role_description,
    u.created_date,
    u._last_updated_at
FROM prd_db.transformed.users u;
