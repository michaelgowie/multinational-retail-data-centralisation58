ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4);

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::UUID;

UPDATE dim_date_times
SET month = CONCAT ('0',month)
WHERE month LIKE '_';

UPDATE dim_date_times
SET day = CONCAT ('0',day)
WHERE day LIKE '_';

ALTER TABLE dim_date_times 
	ADD COLUMN 
		total_timestamp TIMESTAMP;

UPDATE dim_date_times
	SET total_timestamp = 
		TO_TIMESTAMP(
			CONCAT(year,month,day,timestamp),
			'YYYYMMDDHH24:MI:SS'
            );

