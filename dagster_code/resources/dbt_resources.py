"""
dbt Resources Configuration
Each dbt project gets its own resource pointing to its directory and profiles
"""

from dagster_dbt import DbtCliResource

# Ecommerce dbt resource
dbt_nomba = DbtCliResource(
    project_dir="/opt/dagster/app/dbt_project/nomba_dbt",
    profiles_dir="/opt/dagster/app/dbt_project/nomba_dbt"
)

