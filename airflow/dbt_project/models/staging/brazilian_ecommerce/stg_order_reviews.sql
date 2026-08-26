SELECT
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date::DATE AS review_creation_date,
	review_answer_timestamp::TIMESTAMP AS review_answer_timestamp
FROM {{ source('raw', 'olist_order_reviews_dataset') }}