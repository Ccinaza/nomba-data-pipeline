from dagster import ScheduleDefinition
from ..jobs.all_jobs import transactions_job, savings_plan_job, users_job


users_daily_schedule = ScheduleDefinition(
    job=users_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Africa/Lagos",
    description="Runs dim_users daily at 2am Lagos time"
)

savings_plan_daily_schedule = ScheduleDefinition(
    job=savings_plan_job,
    cron_schedule="4 7-18/3 * * *", 
    execution_timezone="Africa/Lagos",
    description="Runs savings_plan mart daily at 4 minutes past every 3rd hour starting from 7am Lagos time"
)

transactions_daily_schedule = ScheduleDefinition(
    job=transactions_job,
    cron_schedule="10 8-19 * * *",
    execution_timezone="Africa/Lagos",
    description="Runs transactions mart daily at 10 minutes past every hour starting from 8am to 7pm Lagos time"
)