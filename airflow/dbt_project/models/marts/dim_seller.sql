SELECT
	'Неизвестен' AS seller_id,
	'Неизвестен' AS seller_city,
	'Неизвестен' AS seller_state
UNION ALL
SELECT
	seller_id,
	seller_city,
	seller_state
FROM {{ ref('stg_sellers') }}