CREATE TABLE IF NOT EXISTS raw.olist_order_payments_dataset(
	order_id VARCHAR(255),
	payment_sequential INTEGER,
	payment_type VARCHAR(255),
	payment_installments INTEGER,
	payment_value DOUBLE PRECISION,
	PRIMARY KEY(order_id,payment_sequential)
);