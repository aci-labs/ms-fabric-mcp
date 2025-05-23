from helpers.utils.context import mcp, __ctx_cache
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    LakehouseClient,
)
# import sempy_labs as labs
# import sempy_labs.lakehouse as slh

from typing import Optional


@mcp.tool()
async def set_lakehouse(lakehouse: str, ctx: Context) -> str:
    """Set the current lakehouse for the session.

    Args:
        lakehouse: Name or ID of the lakehouse
        ctx: Context object containing client information

    Returns:
        A string confirming the lakehouse has been set.
    """
    __ctx_cache[f"{ctx.client_id}_lakehouse"] = lakehouse
    return f"Lakehouse set to '{lakehouse}'."


@mcp.tool()
async def list_lakehouses(workspace: Optional[str] = None, ctx: Context = None) -> str:
    """List all lakehouses in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the list of lakehouses or an error message.
    """
    try:
        client = LakehouseClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        lakehouses = await client.list_lakehouses(
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"]
        )

        return lakehouses

    except Exception as e:
        return f"Error listing lakehouses: {str(e)}"


# @mcp.tool()
# async def list_lakehouses_semantic_link(workspace: Optional[str] = None, ctx: Context = None) -> str:
#     """List all lakehouses in a Fabric workspace using semantic-link-labs."""
#     try:
#         manager = LakehouseManager()
#         lakehouses = manager.list_lakehouses(workspace_id=workspace or __ctx_cache.get(f"{ctx.client_id}_workspace"))
#         markdown = f"# Lakehouses (semantic-link-labs) in workspace '{workspace}'\n\n"
#         markdown += "| ID | Name |\n"
#         markdown += "|-----|------|\n"
#         for lh in lakehouses:
#             markdown += f"| {lh.get('id', 'N/A')} | {lh.get('displayName', 'N/A')} |\n"
#         return markdown
#     except Exception as e:
#         return f"Error listing lakehouses with semantic-link-labs: {str(e)}"

@mcp.tool()
async def create_lakehouse(
    name: str,
    workspace: Optional[str] = None,
    description: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Create a new lakehouse in a Fabric workspace.

    Args:
        name: Name of the lakehouse
        workspace: Name or ID of the workspace (optional)
        description: Description of the lakehouse (optional)
        ctx: Context object containing client information
    Returns:
        A string confirming the lakehouse has been created or an error message.
    """
    try:
        client = LakehouseClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        lakehouse = await client.create_lakehouse(
            name,
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"],
            description,
        )

        return f"Lakehouse '{lakehouse}' created successfully."

    except Exception as e:
        return f"Error creating lakehouse: {str(e)}"
