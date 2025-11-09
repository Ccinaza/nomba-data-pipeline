from dagster_dbt import dbt_assets, DbtCliResource
from dagster import AssetExecutionContext
from .dbt_translator import MultiProjectDbtTranslator

@dbt_assets(
    manifest="/opt/dagster/app/dbt_project/nomba_dbt/target/manifest.json",
    dagster_dbt_translator=MultiProjectDbtTranslator(),
)
def nomba_dbt_assets(context: AssetExecutionContext, dbt_nomba: DbtCliResource):
    context.log.info("Starting nomba dbt build...")
    yield from dbt_nomba.cli(["build"], context=context).stream()
    context.log.info("Completed nomba dbt build!")
