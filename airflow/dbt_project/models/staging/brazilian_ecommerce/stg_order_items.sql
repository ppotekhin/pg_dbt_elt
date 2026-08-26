SELECT
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date::TIMESTAMP AS shipping_limit_date,
	price,
	freight_value
FROM {{ source('raw', 'olist_order_items_dataset') }}