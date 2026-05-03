import os
from pathlib import Path
from dagster_dbt import DbtCliResource, load_assets_from_dbt_manifest, DagsterDbtTranslator
from dagster import AssetExecutionContext, op, asset, multi_asset, AssetOut, Output
import dagster as dg


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_automation_condition(self, dbt_resource_props):
        return dg.AutomationCondition.eager()


PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
DBT_PROJECT_DIR = PROJECT_ROOT / "data_transformation"

# Configure dbt project resource
dbt_warehouse_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

# Generate dbt manifest (only runs dbt parse, NOT dbt deps)
dbt_manifest_path = dbt_warehouse_resource.cli(
    ["--quiet", "parse"], 
    target_path=Path("target")
).wait().target_path.joinpath("manifest.json")

if not dbt_manifest_path.exists():
    raise FileNotFoundError(
        f"dbt manifest not found at {dbt_manifest_path}."
    )


# Load dbt models as Dagster assets (does NOT run dbt deps)
@dg.multi_asset(
    name="forex_dbt_models",
    outs={
        asset_key.path[-1]: AssetOut(key=asset_key)
        for asset_key in load_assets_from_dbt_manifest(
            manifest=dbt_manifest_path,
            dagster_dbt_translator=CustomDagsterDbtTranslator()
        ).keys()
    },
    compute_kind="dbt",
)
def forex_dbt_assets(context: AssetExecutionContext, dbt_warehouse_resource: DbtCliResource):
    """
    Executes dbt build to materialize all dbt models.
    Packages are pre-committed, so no deps needed.
    """
    dbt_cli_invocation = dbt_warehouse_resource.cli(
        ["build", "--full-refresh"], 
        context=context
    )
    
    # Stream dbt events and yield as Dagster events
    yield from dbt_cli_invocation.stream()