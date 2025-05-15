from helpers.utils.context import mcp, __ctx_cache
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    SemanticModelClient,
)
from helpers.logging_config import get_logger

from typing import Optional

logger = get_logger(__name__)

@mcp.tool()
async def list_semantic_models(workspace: Optional[str] = None, ctx: Context = None) -> str:
    """List all semantic models in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the list of semantic models or an error message.
    """
    try:
        client = SemanticModelClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        models = await client.list_semantic_models(
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"]
        )
        
        
        markdown = f"# Semantic Models in workspace '{workspace}'\n\n"
        markdown += "| ID | Name | Folder ID | Description |\n"
        markdown += "|-----|------|-----------|-------------|\n"

        for model in models:
            markdown += f"| {model.get('id', 'N/A')} | {model.get('displayName', 'N/A')} | {model.get('folderId', 'N/A')} | {model.get('description', 'N/A')} |\n"

        return markdown

    except Exception as e:
        return f"Error listing semantic models: {str(e)}"

@mcp.tool()
async def get_semantic_model(
    workspace: Optional[str] = None,
    model_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Get a specific semantic model by ID.

    Args:
        workspace: Name or ID of the workspace (optional)
        model_id: ID of the semantic model (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the details of the semantic model or an error message.
    """
    try:
        client = SemanticModelClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        model = await client.get_semantic_model(
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"],
            model_id if model_id else __ctx_cache[f"{ctx.client_id}_semantic_model"],
        )

        return f"Semantic Model '{model['displayName']}' details:\n\n{model}"

    except Exception as e:
        return f"Error retrieving semantic model: {str(e)}"