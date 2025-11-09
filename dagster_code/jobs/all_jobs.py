from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from ..assets.dbt_assets import nomba_dbt_assets


transactions_job = define_asset_job(
    name="transactions_job",
    selection=build_dbt_asset_selection(
        [nomba_dbt_assets], dbt_select="+fact_savings_transaction"
    ),
    description="Runs fact_savings_transaction mart and all its upstream dependencies",
)

savings_plan_job = define_asset_job(
    name="savings_plan_job",
    selection=build_dbt_asset_selection(
        [nomba_dbt_assets], dbt_select="+dim_savings_plan"
    ),
    description="Runs savings_plan mart and all its upstream dependencies",
)

users_job = define_asset_job(
    name="users_job",
    selection=build_dbt_asset_selection([nomba_dbt_assets], dbt_select="+dim_users"),
    description="Runs dim_users mart and all its upstream dependencies",
)
