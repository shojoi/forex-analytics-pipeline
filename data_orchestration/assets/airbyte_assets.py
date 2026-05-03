from dagster_airbyte import AirbyteWorkspace, build_airbyte_assets_definitions, DagsterAirbyteTranslator
import dagster as dg


class CustomDagsterAirbyteTranslator(DagsterAirbyteTranslator):
    def get_asset_spec(self, props):
        default_spec = super().get_asset_spec(props)
        return default_spec.replace_attributes(
            key=default_spec.key,
            group_name="airbyte",
            automation_condition=dg.AutomationCondition.eager()
        )


# Connect to your  Airbyte instance
airbyte_workspace = AirbyteWorkspace(
    rest_api_base_url=dg.EnvVar("AIRBYTE_REST_API_BASE_URL"),
    configuration_api_base_url=dg.EnvVar("AIRBYTE_CONFIGURATION_API_BASE_URL"),
    workspace_id=dg.EnvVar("AIRBYTE_WORKSPACE_ID"),
    client_id=dg.EnvVar("AIRBYTE_CLIENT_ID"),
    client_secret=dg.EnvVar("AIRBYTE_CLIENT_SECRET"),
)

# Load all assets from your Airbyte workspace
airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_workspace,
    dagster_airbyte_translator=CustomDagsterAirbyteTranslator()
)