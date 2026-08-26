SELECT
	'Неизвестен' AS customer_id,
	'Неизвестен' AS customer_unique_id,
	'Неизвестен' AS customer_city,
	'Неизвестен' AS customer_state
UNION ALL
SELECT
	customer_id,
	customer_unique_id,
	customer_city,
	customer_state
FROM {{ ref('stg_customers') }}