from os import makedirs, path
import sqlparse
from shutil import rmtree
from pathlib import Path
from datetime import timedelta
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kagglehub import KaggleDatasetAdapter, dataset_load
from pandas import DataFrame
from psycopg.cursor import Cursor


@task
def prepare_local_dir(dir_path: str) -> str:

	if path.exists(dir_path):
		rmtree(dir_path)
	makedirs(dir_path)

	return dir_path


@task(
	retries=3,
	retry_delay=timedelta(minutes=1),
	retry_exponential_backoff=True,
	execution_timeout=timedelta(minutes=15)
)
def load_kaggle_dataset_to_csv(
	kaggle_dataset: str,
	kaggle_filepath: str,
	save_path: str
) -> str:

	df: DataFrame = dataset_load(
		KaggleDatasetAdapter.PANDAS,
		kaggle_dataset,
		kaggle_filepath
	)
	df.to_csv(save_path,index=False)
	return save_path


def _handle_preoperator(cursor: Cursor, preoperator_path: str | None) -> None:

	if not preoperator_path or not path.exists(preoperator_path):
		return

	sql_text: str = Path(preoperator_path).read_text()
	statements: list[str] = sqlparse.split(sql_text)
	for stmt in statements:
		stmt = stmt.strip()
		if stmt:
			cursor.execute(stmt)


def _write_csv_to_stage(
		cursor: Cursor,
		table_name: str,
		csv_filepath: str,
		chunk_size: int | None,
		schema: str | None = None
	) -> str:

	staging_table_name: str = f'staging_{table_name}'
	if schema:
		table_name = f'{schema}.{table_name}'
	cursor.execute(f'''
		CREATE TEMPORARY TABLE {staging_table_name}
		(LIKE {table_name} INCLUDING ALL);
	''')
	copy_sql: str = f'''
		COPY {staging_table_name}
		FROM STDIN
		WITH (FORMAT csv, HEADER true, DELIMITER ',')
	'''
	with open(csv_filepath, 'r', encoding='utf-8') as f, cursor.copy(copy_sql) as copy:

		if chunk_size:
			while chunk := f.read(chunk_size):
				copy.write(chunk)
		else:
			copy.write(f.read())

	return staging_table_name


def _clear_target(
		cursor: Cursor,
		table_name: str,
		staging_table_name: str,
		unique_keys: list[str],
		schema: str | None
	) -> None:

	if schema:
		table_name = f'{schema}.{table_name}'

	if unique_keys:
		cursor.execute(f'''
			DELETE FROM {table_name} t
			USING {staging_table_name} s
				WHERE {"\nAND ".join([f"t.{key} = s.{key}" for key in unique_keys])}
		''')
	else:
		cursor.execute(f'TRUNCATE TABLE {table_name}')


def _write_to_target_from_stage(
	cursor: Cursor,
	table_name: str,
	staging_table_name: str,
	schema: str | None
) -> None:

	if schema:
		table_name = f'{schema}.{table_name}'

	cursor.execute(f'''
		INSERT INTO {table_name}
		SELECT * FROM {staging_table_name};
	''')


@task(
	retries=3,
	retry_delay=timedelta(minutes=1),
	retry_exponential_backoff=True,
	execution_timeout=timedelta(minutes=15)
)
def load_from_csv_to_dwh(
	csv_filepath: str,
	postgres_conn_id: str,
	table_name: str,
	unique_keys: list[str],
	schema: str | None = None,
	preoperator_path: str | None = None,
	chunk_size: int | None = None
) -> None:

	pg_hook = PostgresHook(postgres_conn_id)
	if not path.exists(csv_filepath):
		raise FileNotFoundError(f'file://{csv_filepath} not found')

	with pg_hook.get_conn() as conn, conn.cursor() as cursor:

		_handle_preoperator(cursor,preoperator_path)
		staging_table_name = _write_csv_to_stage(cursor,table_name,csv_filepath,chunk_size,schema)
		_clear_target(cursor,table_name,staging_table_name,unique_keys,schema)
		_write_to_target_from_stage(cursor,table_name,staging_table_name,schema)

