CREATE TABLE IF NOT EXISTS raw.olist_order_items_dataset(
	order_id VARCHAR(255),
	order_item_id INTEGER,
	product_id VARCHAR(255),
	seller_id VARCHAR(255),
	shipping_limit_date VARCHAR(255),
	price DOUBLE PRECISION,
	freight_value DOUBLE PRECISION,
	PRIMARY KEY(order_id, order_item_id, product_id, seller_id)
);