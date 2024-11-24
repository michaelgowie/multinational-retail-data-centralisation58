ALTER TABLE dim_card_details
ADD CONSTRAINT prim_constraint PRIMARY KEY (card_number);

ALTER TABLE dim_products
ADD CONSTRAINT primary_product_key PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD CONSTRAINT date_primary_key PRIMARY KEY (date_uuid);

ALTER TABLE dim_store_details
ADD CONSTRAINT store_primary_key PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD CONSTRAINT user_primary_key PRIMARY KEY (user_uuid);


ALTER TABLE orders_table
ADD CONSTRAINT foreign_card
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table 
ADD CONSTRAINT foreign_store
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table 
ADD CONSTRAINT foreign_product
FOREIGN KEY (product_code) REFERENCES dim_products (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT foreign_users
FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table 
ADD CONSTRAINT foreign_date
FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);