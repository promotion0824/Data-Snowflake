-- ********************************************************************************************************************************
-- Create Sites view
-- ********************************************************************************************************************************
CREATE OR REPLACE VIEW published.sites AS
	SELECT
		site_id,
		portfolio_id,
		customer_id,
		name,
		building_id,
		building_name,
		type,
		address_city,
		address_country,
		address_postal_code,
		address_region,
		latitude,
		longitude,
		time_zone   
	FROM transformed.sites;
