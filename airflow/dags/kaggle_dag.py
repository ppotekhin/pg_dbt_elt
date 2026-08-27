from os import environ, path
from airflow.sdk import dag, task_group
from generic_tasks import (
	prepare_local_dir,
	load_kaggle_dataset_to_csv,
	load_from_csv_to_dwh
)
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from resources import table_info_by_source, DataSource
from settings import KAGGLE_DATASET_DIR, DBT_PROJECT_PATH, Connections


@dag(
	schedule=None,
	catchup=False
)
def kaggle_dag():

	@task_group
	def extract_and_load():

		prepared_local_dir: str = prepare_local_dir(KAGGLE_DATASET_DIR)

		for table_name in table_info_by_source[DataSource.KAGGLE].keys():

			@task_group(group_id=f'load_{table_name}')
			def load_kaggle_file(local_dir: str):

				csv_filepath = load_kaggle_dataset_to_csv.override(task_id=f'load_local_{table_name}')(
					table_name,
					local_dir
				)
				load_from_csv_to_dwh.override(task_id=f'load_dwh_{table_name}')(
					csv_filepath=csv_filepath,
					postgres_conn_id=Connections.DWH,
					data_source=DataSource.KAGGLE,
					table_name=table_name
				)

			load_kaggle_file(prepared_local_dir)

	dbt_transformations = DbtTaskGroup(
		group_id='dbt_transformations',
		project_config=ProjectConfig(DBT_PROJECT_PATH),
		execution_config=ExecutionConfig(
			dbt_executable_path=path.join(environ.get('AIRFLOW_HOME'),'dbt_venv/bin/dbt')
		),
		profile_config=ProfileConfig(
			profile_name='pg_dbt_elt',
			target_name='dev',
			profile_mapping=PostgresUserPasswordProfileMapping(
				conn_id=Connections.DWH,
				profile_args={ 'schema': 'public' }
			)
		),
		render_config=RenderConfig(
			select=['+models/marts'],
		),
	)

	extract_and_load() >> dbt_transformations


kaggle_dag()

