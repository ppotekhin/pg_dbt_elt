CREATE TABLE IF NOT EXISTS raw.olist_customers_dataset (
	customer_id VARCHAR(255),
	customer_unique_id VARCHAR(255),
	customer_zip_code_prefix INTEGER,
	customer_city VARCHAR(255),
	customer_state VARCHAR(255),
	PRIMARY KEY(customer_id, customer_unique_id)
);