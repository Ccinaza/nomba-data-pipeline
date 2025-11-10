from dagster_dbt import dbt_assets, DbtCliResource
from dagster import AssetExecutionContext
from .dbt_translator import MultiProjectDbtTranslator


# --- SNAPSHOTS ---
@dbt_assets(
    manifest="/opt/dagster/app/dbt_project/nomba_dbt/target/manifest.json",
    dagster_dbt_translator=MultiProjectDbtTranslator(),
    select="resource_type:snapshot",  # Use resource_type instead of config.materialized
)
def nomba_dbt_snapshots(context: AssetExecutionContext, dbt_nomba: DbtCliResource):
    context.log.info("Starting dbt snapshot run...")
    yield from dbt_nomba.cli(["snapshot"], context=context).stream()
    context.log.info("Completed dbt snapshot run!")


# --- MODELS + TESTS ---
@dbt_assets(
    manifest="/opt/dagster/app/dbt_project/nomba_dbt/target/manifest.json",
    dagster_dbt_translator=MultiProjectDbtTranslator(),
    select="resource_type:model",  # Use resource_type instead of config.materialized
)
def nomba_dbt_models(context: AssetExecutionContext, dbt_nomba: DbtCliResource):
    context.log.info("Starting dbt build (models + tests)...")
    yield from dbt_nomba.cli(["build"], context=context).stream()
    context.log.info("Completed dbt build successfully!")


# from dagster_dbt import dbt_assets, DbtCliResource
# from dagster import AssetExecutionContext
# from .dbt_translator import MultiProjectDbtTranslator


# @dbt_assets(
#     manifest="/opt/dagster/app/dbt_project/nomba_dbt/target/manifest.json",
#     dagster_dbt_translator=MultiProjectDbtTranslator(),
# )
# def nomba_dbt_assets(context: AssetExecutionContext, dbt_nomba: DbtCliResource):
#     context.log.info("Starting dbt snapshot run...")
#     snapshot_result = dbt_nomba.cli(["snapshot"], context=context).stream()
#     yield from snapshot_result
#     context.log.info("Completed dbt snapshot run!")

#     context.log.info("Starting dbt build (models + tests)...")
#     build_result = dbt_nomba.cli(["build"], context=context).stream()
#     yield from build_result
#     context.log.info("Completed dbt build successfully!")
