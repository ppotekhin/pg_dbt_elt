CREATE TABLE IF NOT EXISTS raw.olist_sellers_dataset(
	seller_id VARCHAR(255) PRIMARY KEY,
	seller_zip_code_prefix INTEGER,
	seller_city VARCHAR(255),
	seller_state VARCHAR(255)
);