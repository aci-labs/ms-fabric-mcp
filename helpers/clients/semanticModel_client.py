from helpers.logging_config import get_logger
from helpers.clients.fabric_client import FabricApiClient

logger = get_logger(__name__)


class SemanticModelClient:
    def __init__(self, client: FabricApiClient):
        self.client = client

    async def list_semantic_models(self, workspace_id: str):
        """List all semantic models in a workspace."""
        models = await self.client.get_semantic_models(workspace_id)

        if not models:
            return f"No semantic models found in workspace '{workspace_id}'."

        return models

    async def get_semantic_model(self, workspace_id: str, model_id: str):
        """Get a specific semantic model by ID."""
        model = await self.client.get_semantic_model(workspace_id, model_id)

        if not model:
            return f"No semantic model found with ID '{model_id}' in workspace '{workspace_id}'."

        return model

    # async def get_model_schema(
    #     self,
    #     workspace: str,
    #     rsc_id: str,
    #     rsc_type: str,
    #     table_name: str,
    #     credential: DefaultAzureCredential,
    # ):
    #     """Retrieve schema for a specific model."""

    #     models = await self.list_semantic_models(workspace)

    #     # Find the specific table
    #     matching_tables = [t for t in tables if t["name"].lower() == table_name.lower()]

    #     if not matching_tables:
    #         return f"No table found with name '{table_name}' in {rsc_type} '{rsc_id}'."

    #     table = matching_tables[0]

    #     # Check that it is a Delta table
    #     if table["format"].lower() != "delta":
    #         return f"The table '{table_name}' is not a Delta table (format: {table['format']})."

    #     # Get schema
    #     delta_tables = await get_delta_schemas([table], credential)

    #     if not delta_tables:
    #         return f"Could not retrieve schema for table '{table}'."

    #     # Format result as markdown
    #     table_info, schema, metadata = delta_tables[0]
    #     markdown = format_schema_to_markdown(table_info, schema, metadata)

    #     return markdown

    # async def get_all_schemas(
    #     self,
    #     workspace: str,
    #     rsc_id: str,
    #     rsc_type: str,
    #     credential: DefaultAzureCredential,
    # ):
    #     """Get schemas for all Delta tables in a Fabric lakehouse."""
    #     # Get all tables
    #     tables = await self.list_tables(workspace, rsc_id, rsc_type)

    #     if isinstance(tables, str):
    #         return tables

    #     if not tables:
    #         return f"No tables found in {rsc_type} '{rsc_id}'."

    #     # Filter to only Delta tables
    #     delta_format_tables = [t for t in tables if t["format"].lower() == "delta"]

    #     if not delta_format_tables:
    #         return f"No Delta tables found in {rsc_type} '{rsc_id}'."

    #     # Get schema for all tables
    #     delta_tables = await get_delta_schemas(delta_format_tables, credential)

    #     logger.debug(f"Delta Tables response: {tables}")
    #     if not delta_tables:
    #         return "Could not retrieve schemas for any tables."

    #     # Format the result as markdown
    #     markdown = "# Delta Table Schemas\n\n"
    #     markdown += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    #     markdown += f"Workspace: {workspace}\n"
    #     markdown += f"Lakehouse: {rsc_id}\n\n"

    #     for table_info, schema, metadata in delta_tables:
    #         markdown += format_schema_to_markdown(table_info, schema, metadata)

    #     return markdown
