from os import environ, path
from dataclasses import dataclass, field


PREOPERATORS_PATH = path.join(environ.get('AIRFLOW_HOME'),'dags/sql')


@dataclass
class DatasetInfo:
	dataset_name: str
	unique_keys: list[str] | None = field(default=None)
	filepath: str = field(default=None)
	schema: str = field(default='raw')
	local_filepath: str = field(default=None)
	preoperator_path: str = field(default=None)

	def __post_init__(self):
		if self.filepath is None:
			self.filepath = f'{self.dataset_name}.csv'
			
		if self.preoperator_path is None:
			self.preoperator_path = path.join(
				PREOPERATORS_PATH, 
				f'preoperator_{self.dataset_name}.sql'
			)


@dataclass
class KaggleDataset:

	dataset: str
	filepaths: list[DatasetInfo]


BrazilianEcommerceKaggleDataset = KaggleDataset(
	'olistbr/brazilian-ecommerce',
	[
		DatasetInfo(
			'olist_customers_dataset',
			['customer_id', 'customer_unique_id']
		),
		DatasetInfo('olist_geolocation_dataset'),
		DatasetInfo(
			'olist_order_items_dataset',
			['order_id', 'order_item_id', 'product_id', 'seller_id']
		),
		DatasetInfo(
			'olist_order_payments_dataset',
			['order_id']
		),
		DatasetInfo(
			'olist_order_reviews_dataset',
			['review_id','order_id']
		),
		DatasetInfo(
			'olist_orders_dataset',
			['order_id','customer_id']
		),
		DatasetInfo(
			'olist_products_dataset',
			['product_id']
		),
		DatasetInfo(
			'olist_sellers_dataset',
			['seller_id']
		),
		DatasetInfo('product_category_name_translation')
	]
)

