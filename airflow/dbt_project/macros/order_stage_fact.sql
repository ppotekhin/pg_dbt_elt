{% macro order_stage_fact(timestamp_column) %}
SELECT
    {{ timestamp_column }},
    order_id,
    product_id,
    seller_id,
    customer_id,
    CASE WHEN product_id = 'Неизвестен' THEN null ELSE COUNT(product_id)::SMALLINT END AS item_count,
    price,
    freight_value
FROM {{ ref('int_order_with_item') }}
WHERE {{ timestamp_column }} IS NOT NULL
GROUP BY {{ timestamp_column }}, order_id, product_id, seller_id, customer_id, price, freight_value
{% endmacro %}