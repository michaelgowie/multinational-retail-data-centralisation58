


UPDATE dim_products
SET product_price = RIGHT(product_price, LENGTH(product_price) - 1);

ALTER TABLE dim_products
ADD weight_class VARCHAR(30);

UPDATE dim_products
SET

	weight_class = 'Truck_Required' WHERE weight > 140;

UPDATE dim_products
SET

	weight_class = 'Heavy' WHERE weight BETWEEN 40 AND 140;

UPDATE dim_products
SET

	weight_class = 'Mid_sized' WHERE weight BETWEEN 2 AND 40;

UPDATE dim_products
SET

	weight_class = 'Light' WHERE weight < 2;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE NUMERIC USING product_price::numeric;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE NUMERIC USING weight::numeric;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);


ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);


ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;


ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;


ALTER TABLE dim_products
ADD COLUMN still_available BOOL;

UPDATE dim_products
SET still_available = False
WHERE product_code IN(
SELECT product_code 	
FROM dim_products
WHERE removed = 'Removed'
);

UPDATE dim_products
SET still_available = True
WHERE product_code IN(
SELECT product_code	
FROM dim_products
WHERE removed != 'Removed'
);
