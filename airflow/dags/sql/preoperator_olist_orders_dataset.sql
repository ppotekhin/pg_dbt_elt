CREATE TABLE IF NOT EXISTS raw.olist_orders_dataset(
	order_id VARCHAR(255),
	customer_id VARCHAR(255),
	order_status VARCHAR(255),
	order_purchase_timestamp VARCHAR(255),
	order_approved_at VARCHAR(255),
	order_delivered_carrier_date VARCHAR(255),
	order_delivered_customer_date VARCHAR(255),
	order_estimated_delivery_date VARCHAR(255),
	PRIMARY KEY(order_id,customer_id)
);