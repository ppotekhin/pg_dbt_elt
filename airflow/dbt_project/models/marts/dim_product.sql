SELECT 'Неизвестен' AS product_id, 'Неизвестен' AS product_category_name
UNION ALL
SELECT
	product_id,
	product_category_name
FROM {{ ref('stg_products') }}