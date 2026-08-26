from os import path, environ
from enum import StrEnum

SAVEFILES_DIR: str = '/tmp'
KAGGLE_DATASET_DIR: str = path.join(SAVEFILES_DIR,'kaggle')
DBT_PROJECT_PATH: str = path.join(environ.get('AIRFLOW_HOME'),'dbt_project')


class Connections(StrEnum):
	DWH = 'dwh'

