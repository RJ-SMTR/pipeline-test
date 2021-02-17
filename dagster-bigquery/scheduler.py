from dagster import daily_schedule, repository
import yaml
from add_gps_data import add_data_pipeline
from datetime import datetime, time


def read_config(yaml_file):
    with open(yaml_file, "r") as load_file:
        config = yaml.load(load_file, Loader=yaml.FullLoader)
        return config

@daily_schedule(
    pipeline_name="add_data_pipeline",
    start_date=datetime(2020, 6, 1),
    execution_time=time(19, 16),
    execution_timezone="America/Sao_Paulo",
)
def good_morning_schedule(date):
    return read_config('add_data_config.yaml')

@repository
def add_data_repository():
    return [add_data_pipeline, good_morning_schedule]