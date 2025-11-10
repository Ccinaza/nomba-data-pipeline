from dagster import Definitions, load_assets_from_modules
from . import assets
from .jobs.all_jobs import (
    users_job,
    transactions_job,
    savings_plan_job,
    users_extraction_job,
    savings_plans_extraction_job,
    savings_transactions_extraction_job
)
from .schedules.all_schedules import (
    transactions_daily_schedule,
    users_daily_schedule,
    savings_plan_daily_schedule,
    raw_users_daily_extraction_schedule,
    raw_savings_plans_extraction_schedule,
    raw_savings_transactions_extraction_schedule
)
from .resources.dbt_resources import dbt_nomba

# Load all assets
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[
        savings_plan_job,
        users_job,
        transactions_job,
        users_extraction_job,
        savings_plans_extraction_job,
        savings_transactions_extraction_job

    ],

    schedules=[
        transactions_daily_schedule,
        users_daily_schedule,
        savings_plan_daily_schedule,
        raw_savings_plans_extraction_schedule,
        raw_savings_transactions_extraction_schedule,
        raw_users_daily_extraction_schedule
    ],

    resources={
        "dbt_nomba": dbt_nomba,
    },
)