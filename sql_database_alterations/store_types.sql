ALTER TABLE dim_store_details
	DROP COLUMN lat;


UPDATE dim_store_details
SET 
	longitude = NULL,
	address = NULL,
	locality = NULL
	WHERE store_code = 'WEB-1388012W';

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE NUMERIC USING longitude::numeric;

ALTER TABLE dim_store_details
	ALTER COLUMN latitude TYPE NUMERIC USING latitude::numeric;

ALTER TABLE dim_store_details
	ALTER COLUMN locality TYPE VARCHAR(255);
SELECT MAX(LENGTH(store_code)) FROM dim_store_details;

ALTER TABLE dim_store_details
	ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;

ALTER TABLE dim_store_details
	ALTER COLUMN opening_date TYPE Date USING opening_date::DATE;

ALTER TABLE dim_store_details
	ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
	ALTER COLUMN country_code TYPE VARCHAR(3);

ALTER TABLE dim_store_details
	ALTER COLUMN continent TYPE VARCHAR(255);