from os import environ, path
from airflow.sdk import dag, task_group
from generic_tasks import (
	prepare_local_dir,
	load_kaggle_dataset_to_csv,
	load_from_csv_to_dwh
)
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from resources import BrazilianEcommerceKaggleDataset
from settings import KAGGLE_DATASET_DIR, DBT_PROJECT_PATH, Connections


@dag
def kaggle_dag():

	@task_group
	def extract_and_load():

		prepare_local_dir_task = prepare_local_dir(KAGGLE_DATASET_DIR)

		for kaggle_file in BrazilianEcommerceKaggleDataset.filepaths:

			@task_group(group_id=f'load_{kaggle_file.dataset_name}')
			def load_kaggle_file():

				load_to_csv_task = load_kaggle_dataset_to_csv.override(task_id=f'load_local_{kaggle_file.dataset_name}')(
					BrazilianEcommerceKaggleDataset.dataset,
					kaggle_file.filepath,
					path.join(KAGGLE_DATASET_DIR,kaggle_file.filepath)
				)
				load_from_csv_to_dwh.override(task_id=f'load_dwh_{kaggle_file.dataset_name}')(
					csv_filepath=load_to_csv_task,
					postgres_conn_id=Connections.DWH,
					table_name=kaggle_file.dataset_name,
					unique_keys=kaggle_file.unique_keys,
					preoperator_path=kaggle_file.preoperator_path,
					schema=kaggle_file.schema
				)

			prepare_local_dir_task >> load_kaggle_file()

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

