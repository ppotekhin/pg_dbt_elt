CREATE TABLE IF NOT EXISTS raw.olist_geolocation_dataset(
	geolocation_zip_code_prefix INTEGER,
	geolocation_lat DOUBLE PRECISION,
	geolocation_lng DOUBLE PRECISION,
	geolocation_city VARCHAR(255),
	geolocation_state VARCHAR(255)
);