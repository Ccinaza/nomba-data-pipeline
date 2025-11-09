from dagster_dbt import DagsterDbtTranslator
from dagster import AssetKey

class MultiProjectDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        """
        This method determines how assets are grouped in the Dagster UI.
        Groups assets by project (ecommerce or pillar) so that each project
        shows its own folder structure (poc_staging, poc_intermediate, poc_mart).
        """
        
        # fqn = Fully Qualified Name - shows the hierarchical path
        fqn = dbt_resource_props["fqn"]
        
        # Check if we have at least 1 part (project name)
        if len(fqn) >= 1:
            project_name = fqn[0]  # First part: 'ecommerce_dbt' or 'pillar_dbt'
            
            # Clean up project names for nicer display in UI
            # Remove '_dbt' suffix
            clean_project = project_name.replace('_dbt', '')
            
            return clean_project
        
        # Fallback for resources without proper fqn structure
        return "default"
    
    def get_asset_key(self, dbt_resource_props):
        """
        This method determines the unique identifier for each asset in Dagster.
        We're customizing it to include the project name and layer for clarity.
        """
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        fqn = dbt_resource_props["fqn"]
        
        # Get project name and layer from fqn
        project_name = fqn[0].replace('_dbt', '')  
        layer = fqn[1] if len(fqn) >= 2 else 'default'
        
        # For source assets (raw tables)
        if resource_type == "source":
            # Create asset key like: 'nomba/staging/raw_users'
            return AssetKey([project_name, layer, f"raw_{name}"])
       
        return AssetKey([project_name, layer, name])