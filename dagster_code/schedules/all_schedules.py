from dagster import ScheduleDefinition
from ..jobs.all_jobs import transactions_job, savings_plan_job, users_job, users_extraction_job, savings_plans_extraction_job, savings_transactions_extraction_job


raw_users_daily_extraction_schedule = ScheduleDefinition(
    job=users_extraction_job,
    cron_schedule="40 1 * * *",
    execution_timezone="Africa/Lagos",
    description="Runs raw_users daily at 1:40am Lagos time",
)

users_daily_schedule = ScheduleDefinition(
    job=users_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Africa/Lagos",
    description="Runs dim_users daily at 2am Lagos time",
)

raw_savings_plans_extraction_schedule = ScheduleDefinition(
    job=savings_plans_extraction_job,
    cron_schedule="2 7-18/3 * * *",
    execution_timezone="Africa/Lagos",
    description="Runs savings_plans daily at 2 minutes past every 3rd hour starting from 7am Lagos time",
)

savings_plan_daily_schedule = ScheduleDefinition(
    job=savings_plan_job,
    cron_schedule="5 7-18/3 * * *",
    execution_timezone="Africa/Lagos",
    description="Runs savings_plan mart daily at 5 minutes past every 3rd hour starting from 7am Lagos time",
)

raw_savings_transactions_extraction_schedule = ScheduleDefinition(
    job=savings_transactions_extraction_job,
    cron_schedule="5 * * * *",
    execution_timezone="Africa/Lagos",
    description="Runs transactions mart every 4 minutes Lagos time",
)

transactions_daily_schedule = ScheduleDefinition(
    job=transactions_job,
    cron_schedule="10 * * * *",
    execution_timezone="Africa/Lagos",
    description="Runs transactions mart every 5 minutes Lagos time",
)
