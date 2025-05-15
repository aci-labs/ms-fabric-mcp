from helpers.utils.validators import is_valid_uuid
from helpers.logging_config import get_logger
from helpers.clients import FabricApiClient

logger = get_logger(__name__)

class LakehouseClient:
    def __init__(self, client: FabricApiClient):
        self.client = client

    async def list_lakehouses(self, workspace: str):
        """List all lakehouses in a workspace."""
        if not is_valid_uuid(workspace):
            raise ValueError("Invalid workspace ID.")
        lakehouses = await self.client.get_lakehouses(workspace)

        if not lakehouses:
            return f"No lakehouses found in workspace '{workspace}'."

        markdown = f"# Lakehouses in workspace '{workspace}'\n\n"
        markdown += "| ID | Name |\n"
        markdown += "|-----|------|\n"

        for lh in lakehouses:
            markdown += f"| {lh['id']} | {lh['displayName']} |\n"

        return markdown

    async def resolve_lakehouse(self, workspace_id: str, lakehouse_name: str):
        """Resolve lakehouse name to lakehouse ID."""
        return await self.client.resolve_lakehouse(workspace_id, lakehouse_name)
