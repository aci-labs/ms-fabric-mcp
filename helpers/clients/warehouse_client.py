from helpers.logging_config import get_logger
from helpers.clients import FabricApiClient

logger = get_logger(__name__)

class WarehouseClient:
    def __init__(self, client: FabricApiClient):
        self.client = client

    async def list_warehouses(self, workspace: str):
        """List all warehouses in a lakehouse."""
        warehouses = await self.client.get_warehouses(workspace)

        if not warehouses:
            return f"No warehouses found in workspace '{workspace}'."

        markdown = f"# Warehouses in workspace '{workspace}'\n\n"
        markdown += "| ID | Name |\n"
        markdown += "|-----|------|\n"

        for wh in warehouses:
            markdown += f"| {wh['id']} | {wh['displayName']} |\n"

        return markdown
        

