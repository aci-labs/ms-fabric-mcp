from helpers.utils.context import mcp, __ctx_cache
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    NotebookClient,
)
import json
from helpers.logging_config import get_logger


from typing import Optional

logger = get_logger(__name__)


@mcp.tool()
async def list_notebooks(workspace: Optional[str] = None, ctx: Context = None) -> str:
    """List all notebooks in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        ctx: Context object containing client information
    Returns:
        A string containing the list of notebooks or an error message.
    """

    try:
        if ctx is None:
            raise ValueError("Context (ctx) must be provided.")

        notebook_client = NotebookClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )
        return await notebook_client.list_notebooks(workspace)
    except Exception as e:
        logger.error(f"Error listing notebooks: {str(e)}")
        return f"Error listing notebooks: {str(e)}"


@mcp.tool()
async def create_notebook(
    workspace: str,
    # notebook_name: str,
    # content: str,
    ctx: Context = None,
) -> str:
    """Create a new notebook in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace
        notebook_name: Name of the new notebook
        content: Content of the notebook (in JSON format)
        ctx: Context object containing client information
    Returns:
        A string containing the ID of the created notebook or an error message.
    """
    notebook_json = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": [
            {
                "cell_type": "code",
                "source": ["print('Hello, Fabric!')\n"],
                "execution_count": None,
                "outputs": [],
                "metadata": {},
            }
        ],
        "metadata": {"language_info": {"name": "python"}},
    }
    notebook_content = json.dumps(notebook_json)
    try:
        if ctx is None:
            raise ValueError("Context (ctx) must be provided.")

        notebook_client = NotebookClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )
        response = await notebook_client.create_notebook(
            workspace, "test_notebook_2", notebook_content
        )
        return response.get("id", "")  # Return the notebook ID or an empty string
    except Exception as e:
        logger.error(f"Error creating notebook: {str(e)}")
        return f"Error creating notebook: {str(e)}"
