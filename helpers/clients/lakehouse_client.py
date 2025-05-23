from helpers.utils import _is_valid_uuid
from helpers.logging_config import get_logger
from helpers.clients.fabric_client import FabricApiClient
from typing import Optional, Dict, Any

logger = get_logger(__name__)


class LakehouseClient:
    def __init__(self, client: FabricApiClient):
        self.client = client

    async def list_lakehouses(self, workspace: str):
        """List all lakehouses in a workspace."""
        if not _is_valid_uuid(workspace):
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

    async def get_lakehouse(
        self,
        workspace: str,
        lakehouse: str,
    ) -> Optional[Dict[str, Any]]:
        """Get details of a specific lakehouse."""
        if not _is_valid_uuid(workspace):
            raise ValueError("Invalid workspace ID.")

        if not lakehouse:
            raise ValueError("Lakehouse name cannot be empty.")

        response = await self.client.get_item(workspace_id=workspace, item_id=lakehouse)
        logger.info(f"Lakehouse details: {response}")
        return response

    async def resolve_lakehouse(self, workspace_id: str, lakehouse_name: str):
        """Resolve lakehouse name to lakehouse ID."""
        return await self.client.resolve_item_name_and_id(
            workspace=workspace_id, item=lakehouse_name, type="Lakehouse"
        )

    async def create_lakehouse(
        self,
        name: str,
        workspace: str,
        description: Optional[str] = None,
    ):
        """Create a new lakehouse."""
        if not _is_valid_uuid(workspace):
            raise ValueError("Invalid workspace ID.")

        if not name:
            raise ValueError("Lakehouse name cannot be empty.")

        return await self.client.create_item(
            name=name, workspace=workspace, description="description", type="Lakehouse"
        )
