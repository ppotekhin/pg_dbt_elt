from os import environ, path
from dataclasses import dataclass, field
from enum import StrEnum


PREOPERATORS_PATH = path.join(environ.get('AIRFLOW_HOME'),'dags','sql')


class DataSource(StrEnum):
	KAGGLE = 'kaggle'


@dataclass(kw_only=True)
class TableInfo:
	table_name: str
	schema: str = field(default='raw')
	unique_keys: list[str] | None = field(default=None)
	preoperator_path: str = field(default=None)

	def __post_init__(self):
		if self.preoperator_path is None:
			self.preoperator_path = path.join(
				PREOPERATORS_PATH, 
				f'preoperator_{self.table_name}.sql'
			)


table_info_by_source: dict[DataSource,dict[str,TableInfo]] = dict()


@dataclass(kw_only=True)
class KaggleTableInfo(TableInfo):
	kaggle_dataset: str
	kaggle_filepath: str = field(default=None)

	def __post_init__(self):
		super().__post_init__()
		if self.kaggle_filepath is None:
			self.kaggle_filepath = f'{self.table_name}.csv'


table_info_by_source[DataSource.KAGGLE] = {
	'olist_customers_dataset': KaggleTableInfo(
		table_name='olist_customers_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce',
		unique_keys=['customer_id', 'customer_unique_id']
	),
	'olist_geolocation_dataset': KaggleTableInfo(
		table_name='olist_geolocation_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce'
	),
	'olist_order_items_dataset': KaggleTableInfo(
		table_name='olist_order_items_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce',
		unique_keys=['order_id', 'order_item_id', 'product_id', 'seller_id']
	),
	'olist_order_payments_dataset': KaggleTableInfo(
		table_name='olist_order_payments_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce',
		unique_keys=['order_id']
	),
	'olist_order_reviews_dataset': KaggleTableInfo(
		table_name='olist_order_reviews_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce',
		unique_keys=['review_id','order_id']
	),
	'olist_orders_dataset': KaggleTableInfo(
		table_name='olist_orders_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce',
		unique_keys=['order_id','customer_id']
	),
	'olist_products_dataset': KaggleTableInfo(
		table_name='olist_products_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce',
		unique_keys=['product_id']
	),
	'olist_sellers_dataset': KaggleTableInfo(
		table_name='olist_sellers_dataset',
		kaggle_dataset='olistbr/brazilian-ecommerce',
		unique_keys=['seller_id']
	),
	'product_category_name_translation': KaggleTableInfo(
		table_name='product_category_name_translation',
		kaggle_dataset='olistbr/brazilian-ecommerce'
	)
}

