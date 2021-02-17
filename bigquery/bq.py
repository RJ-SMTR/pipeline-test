from dagster_pandas import DataFrame
from dagster_gcp.bigquery.configs import define_bigquery_query_config
from dagster_gcp.bigquery.solids import _preprocess_config
from google.cloud.bigquery.job import QueryJobConfig
from dagster import solid, pipeline

@solid(
    config=define_bigquery_query_config(), required_resource_keys={'bigquery'},
)
def bq_query_input_solid(context, sql_queries: List[str]) -> List[DataFrame]:
    query_job_config = _preprocess_config(context.solid_config.get('query_job_config', {}))

    results = []
    for sql_query in sql_queries:
        cfg = QueryJobConfig(**query_job_config) if query_job_config else None
        context.log.info(
            'executing query %s with config: %s'
            % (sql_query, cfg.to_api_repr() if cfg else '(no config provided)')
        )
        results.append(context.resources.bigquery.query(sql_query, job_config=cfg).to_dataframe())

    return results

@pipeline
def bq_pipeline():
    bq_query_input_solid()