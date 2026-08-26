SELECT
	ordr.order_purchase_timestamp,
	ordr.order_approved_at,
	ordr.order_delivered_carrier_timestamp,
	ordr.order_delivered_customer_timestamp,

	order_id,
	itm.order_item_id,

	COALESCE(itm.product_id,'Неизвестен') AS product_id,
	COALESCE(itm.seller_id,'Неизвестен') AS seller_id,
	COALESCE(ordr.customer_id,'Неизвестен') AS customer_id,

	itm.shipping_limit_date,
	ordr.order_estimated_delivery_date,

	itm.price,
	itm.freight_value
FROM {{ ref('stg_orders') }} ordr
LEFT JOIN {{ ref('stg_order_items') }} itm
	USING(order_id)