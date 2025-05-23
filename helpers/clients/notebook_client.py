from helpers.utils import _is_valid_uuid
from helpers.logging_config import get_logger
from helpers.clients.fabric_client import FabricApiClient
from typing import Dict, Any

logger = get_logger(__name__)


class NotebookClient:
    def __init__(self, client: FabricApiClient):
        self.client = client

    async def list_notebooks(self, workspace: str):
        """List all notebooks in a workspace."""
        if not _is_valid_uuid(workspace):
            raise ValueError("Invalid workspace ID.")
        notebooks = await self.client.get_notebooks(workspace)

        if not notebooks:
            return f"No notebooks found in workspace '{workspace}'."

        markdown = f"# Notebooks in workspace '{workspace}'\n\n"
        markdown += "| ID | Name |\n"
        markdown += "|-----|------|\n"

        for nb in notebooks:
            markdown += f"| {nb['id']} | {nb['displayName']} |\n"

        return markdown

    async def get_notebook(self, workspace: str, notebook_id: str) -> Dict[str, Any]:
        """Get a specific notebook by ID."""
        if not _is_valid_uuid(workspace):
            raise ValueError("Invalid workspace ID.")
        if not _is_valid_uuid(notebook_id):
            raise ValueError("Invalid notebook ID.")

        notebook = await self.client.get_notebook(workspace, notebook_id)

        if not notebook:
            return (
                f"No notebook found with ID '{notebook_id}' in workspace '{workspace}'."
            )

        return notebook

    async def create_notebook(
        self, workspace: str, notebook_name: str, content: str
    ) -> Dict[str, Any]:
        """Create a new notebook."""
        workspace, workspace_id = await self.client.resolve_workspace_name_and_id(
            workspace
        )
        if not workspace_id:
            raise ValueError("Invalid workspace ID.")
        logger.info(f"Creating notebook '{notebook_name}' in workspace '{workspace}'.")
        response = await self.client.create_notebook(
            workspace_id=workspace_id,
            notebook_name=notebook_name,
            ipynb_name=notebook_name,
            content=content,
        )

        if not response.get("id"):
            return f"Failed to create notebook '{notebook_name}' in workspace '{workspace}'."

        return response
