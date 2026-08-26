CREATE TABLE IF NOT EXISTS raw.olist_products_dataset(
	product_id VARCHAR(255) PRIMARY KEY,
	product_category_name VARCHAR(255),
	product_name_lenght REAL,
	product_description_lenght REAL,
	product_photos_qty REAL,
	product_weight_g REAL,
	product_length_cm REAL,
	product_height_cm REAL,
	product_width_cm REAL
);