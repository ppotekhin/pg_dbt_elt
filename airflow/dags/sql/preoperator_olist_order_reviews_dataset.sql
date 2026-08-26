CREATE TABLE IF NOT EXISTS raw.olist_order_reviews_dataset(
	review_id VARCHAR(255),
	order_id VARCHAR(255),
	review_score SMALLINT,
	review_comment_title VARCHAR(255),
	review_comment_message VARCHAR(255),
	review_creation_date VARCHAR(255),
	review_answer_timestamp VARCHAR(255),
	PRIMARY KEY(review_id,order_id)
);