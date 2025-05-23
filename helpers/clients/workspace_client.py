from helpers.logging_config import get_logger
from helpers.clients.fabric_client import FabricApiClient

logger = get_logger(__name__)


class WorkspaceClient:
    def __init__(self, client: FabricApiClient):
        self.client = client

    async def list_workspaces(self):
        """List all available workspaces."""
        workspaces = await self.client.get_workspaces()
        if not workspaces:
            raise ValueError("No workspaces found.")

        markdown = "# Fabric Workspaces\n\n"
        markdown += "| ID | Name | Capacity |\n"
        markdown += "|-----|------|----------|\n"

        for ws in workspaces:
            markdown += f"| {ws['id']} | {ws['displayName']} | {ws.get('capacityId', 'N/A')} |\n"

        return markdown

    async def resolve_workspace(self, workspace_name: str):
        """Resolve workspace name to workspace ID."""
        return await self.client.resolve_workspace_name_and_id(workspace=workspace_name)
