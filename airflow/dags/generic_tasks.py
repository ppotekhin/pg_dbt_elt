from os import makedirs, path, PathLike
from logging import getLogger, Logger
import sqlparse
from shutil import rmtree
from pathlib import Path
from datetime import timedelta
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kagglehub import KaggleDatasetAdapter, dataset_load
from pandas import DataFrame
from psycopg.sql import SQL, Identifier
from psycopg.cursor import Cursor
from resources import KaggleTableInfo, TableInfo, DataSource, table_info_by_source


logger: Logger = getLogger(__name__)


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
	table_name: str,
	save_dir: str | PathLike[str]
) -> str:

	kaggle_file: KaggleTableInfo = table_info_by_source[DataSource.KAGGLE][table_name]
	df: DataFrame = dataset_load(
		KaggleDatasetAdapter.PANDAS,
		kaggle_file.kaggle_dataset,
		kaggle_file.kaggle_filepath
	)
	save_path: str = path.join(save_dir,f'{kaggle_file.table_name}.csv')
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
		table_ident: Identifier,
		staging_ident: Identifier,
		csv_filepath: str,
		chunk_size: int | None
	) -> int:

	create_tmp_stmt: SQL = SQL('''
		CREATE TEMPORARY TABLE {staging}
		(LIKE {table} INCLUDING ALL);
	''').format(staging=staging_ident,table=table_ident)
	cursor.execute(create_tmp_stmt)
	copy_stmt: SQL = SQL('''
		COPY {staging}
		FROM STDIN
		WITH (FORMAT csv, HEADER true, DELIMITER ',')
	''').format(staging=staging_ident)

	with open(csv_filepath, 'r', encoding='utf-8') as f, cursor.copy(copy_stmt) as copy:
		if chunk_size:
			while chunk := f.read(chunk_size):
				copy.write(chunk)
		else:
			copy.write(f.read())

	return cursor.rowcount


def _delete_conflict_rows(
	cursor: Cursor,
	table_ident: Identifier,
	staging_ident: Identifier,
	unique_keys: list[str]
) -> int:
	delete_stmt: SQL = SQL('''
		DELETE FROM {table} t
		USING {staging} s
			WHERE {eq_conditions}
	''').format(
		table=table_ident,
		staging=staging_ident,
		eq_conditions=SQL(' AND ').join(
			SQL('t.{key} = s.{key}').format(key=Identifier(key))
			for key in unique_keys
		)
	)
	cursor.execute(delete_stmt)
	return cursor.rowcount


def _truncate_table(
	cursor: Cursor,
	table_ident: Identifier
) -> int:
	row_count_stmt: SQL = SQL('SELECT COUNT(*) FROM {}').format(table_ident)
	cursor.execute(row_count_stmt)
	pre_clear_row_count: int = cursor.fetchone()[0]
	truncate_stmt: SQL = SQL('TRUNCATE TABLE {table}').format(table=table_ident)
	cursor.execute(truncate_stmt)
	return pre_clear_row_count


def _write_to_target_from_stage(
	cursor: Cursor,
	table_ident: Identifier,
	staging_ident: Identifier
) -> None:
	insert_stmt: SQL = SQL('''
		INSERT INTO {table}
		SELECT * FROM {staging};
	''').format(table=table_ident,staging=staging_ident)
	cursor.execute(insert_stmt)


@task(
	retries=3,
	retry_delay=timedelta(minutes=1),
	retry_exponential_backoff=True,
	execution_timeout=timedelta(minutes=15)
)
def load_from_csv_to_dwh(
	csv_filepath: str,
	postgres_conn_id: str,
	data_source: DataSource,
	table_name: str,
	chunk_size: int | None = None
) -> None:

	pg_hook = PostgresHook(postgres_conn_id)
	if not path.exists(csv_filepath):
		raise FileNotFoundError(f'file://{csv_filepath} not found')

	with pg_hook.get_conn() as conn, conn.cursor() as cursor:

		table_info: TableInfo = table_info_by_source[data_source][table_name]
		table_ident: Identifier = Identifier(table_info.schema, table_info.table_name) \
			if table_info.schema \
			else Identifier(table_info.table_name)
		staging_ident: Identifier = Identifier(f'staging_{table_info.table_name}')

		_handle_preoperator(cursor,table_info.preoperator_path)
		inserted_row_count: int = _write_csv_to_stage(cursor,table_ident,staging_ident,csv_filepath,chunk_size)
		logger.info(f'Создана промежуточная таблица {staging_ident.as_string(cursor)}. Записано {inserted_row_count} строк.')
		deleted_row_count: int | None = None
		if table_info.unique_keys:
			deleted_row_count = _delete_conflict_rows(cursor,table_ident,staging_ident,table_info.unique_keys)
		else:
			deleted_row_count = _truncate_table(cursor,table_ident)
		logger.info(f'Удалено {deleted_row_count} строк в таблице {table_ident.as_string(cursor)}.')
		_write_to_target_from_stage(cursor,table_ident,staging_ident)

