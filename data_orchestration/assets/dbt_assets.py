import os
from pathlib import Path
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from dagster import AssetExecutionContext
import dagster as dg


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_automation_condition(self, dbt_resource_props):
        return dg.AutomationCondition.eager()


PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
DBT_PROJECT_DIR = PROJECT_ROOT / "data_transformation"

dbt_warehouse_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

dbt_manifest_path = dbt_warehouse_resource.cli(
    ["--quiet", "parse"], 
    target_path=Path("target")
).wait().target_path.joinpath("manifest.json")

if not dbt_manifest_path.exists():
    raise FileNotFoundError(f"dbt manifest not found at {dbt_manifest_path}.")


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def forex_dbt_assets(context: AssetExecutionContext, dbt_warehouse_resource: DbtCliResource):
    """
    dbt assets for forex analytics pipeline.
    Packages are committed, so deps validates quickly.
    """
    yield from dbt_warehouse_resource.cli(
        ["build", "--full-refresh"], 
        context=context
    ).stream()