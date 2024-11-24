ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(5);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

UPDATE dim_card_details
SET card_number = RIGHT(card_number, LENGTH(card_number) - 4) WHERE card_number LIKE '????%%';

UPDATE dim_card_details
SET card_number = RIGHT(card_number, LENGTH(card_number) - 3) WHERE card_number LIKE '???%%';

UPDATE dim_card_details
SET card_number = RIGHT(card_number, LENGTH(card_number) - 2) WHERE card_number LIKE '??%%';

UPDATE dim_card_details
SET card_number = RIGHT(card_number, LENGTH(card_number) - 1) WHERE card_number LIKE '?%%';