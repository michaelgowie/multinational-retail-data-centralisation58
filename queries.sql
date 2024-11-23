SELECT 
    country_code,
    COUNT(latitude) AS total_stores
    FROM dim_store_details
    GROUP BY  country_code
    ORDER BY total_stores DESC;


SELECT locality,
    COUNT(store_code) AS total_stores
    FROM dim_store_details
    GROUP BY locality
    ORDER BY total_stores DESC
    LIMIT 7;


SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales_in_month,
    dim_date_times.month
    FROM orders_table
        JOIN dim_products ON orders_table.product_code = dim_products.product_code
        JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
        GROUP BY dim_date_times.month
        ORDER BY total_sales_in_month DESC;

SELECT 
    COUNT(orders_table.index) AS total_num_of_sales,
    SUM(orders_table.product_quantity) AS total_products_sold,
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales_amount,
    CASE 
            WHEN dim_store_details.store_type = 'Web Portal' THEN 'online'
            ELSE 'in person'
			END AS online_or_in_person
    FROM orders_table
        JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
        JOIN dim_products ON dim_products.product_code = orders_table.product_code
        GROUP BY CASE 
            WHEN dim_store_details.store_type = 'Web Portal' THEN 'online'
            ELSE 'in person'
            END;

SELECT 
    dim_store_details.store_type,
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales_per_type,
    ROUND(100 * 
    (SUM(dim_products.product_price * orders_table.product_quantity) / 
	(
    SELECT SUM (dim_products.product_price * orders_table.product_quantity)
    FROM orders_table
    JOIN dim_products ON dim_products.product_code = orders_table.product_code
	)
    ), 1) AS sales_made_percentage
    FROM orders_table
    JOIN dim_products ON dim_products.product_code = orders_table.product_code
    JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
    GROUP BY store_type
    ORDER BY sales_made_percentage DESC;

SELECT 
    dim_date_times.year,
    dim_date_times.month,
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales_in_month
    FROM dim_date_times
    JOIN orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
    JOIN dim_products ON orders_table.product_code = dim_products.product_code 
    GROUP BY dim_date_times.month, dim_date_times.year
    ORDER BY total_sales_in_month DESC;

SELECT 
    country_code,
    SUM(staff_numbers) as total_staff
    FROM dim_store_details
    GROUP BY country_code;

SELECT
    dim_store_details.country_code,
    dim_store_details.store_type,
    SUM(dim_products.product_price * orders_table.product_quantity) AS total_revenue_by_type
    FROM orders_table
    JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
    JOIN dim_products ON dim_products.product_code = orders_table.product_code
    WHERE country_code = 'DE'
    GROUP BY store_type, country_code 
    ORDER BY total_revenue_by_type;

CREATE OR REPLACE VIEW interval_view AS
SELECT 
	LEAD(total_timestamp) OVER (ORDER BY total_timestamp) - total_timestamp AS time_til_next_order,
	year
	FROM dim_date_times
	ORDER BY time_til_next_order DESC;

SELECT 
	year,
	AVG(time_til_next_order) AS avg_interval
	FROM interval_view
	GROUP BY year
	ORDER BY avg_interval DESC;

