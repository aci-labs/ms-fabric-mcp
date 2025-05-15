from helpers.utils.context import mcp, __ctx_cache, ctx
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    LakehouseClient,
)

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
