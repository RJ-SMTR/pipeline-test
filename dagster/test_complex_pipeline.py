from complex_pipeline import complex_pipeline
from dagster import execute_pipeline
import yaml

def read_config(yaml_file):
    with open(yaml_file, "r") as load_file:
        config = yaml.load(load_file, Loader=yaml.FullLoader)
        return config

def test_complex_pipeline():
    res = execute_pipeline(complex_pipeline, read_config("complex_pipeline_config.yaml"))
    assert res.success
    assert len(res.solid_result_list) == 4
    for solid_res in res.solid_result_list:
        assert solid_res.success
