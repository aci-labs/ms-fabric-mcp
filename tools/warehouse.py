from helpers.utils.context import mcp, __ctx_cache, ctx
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    WarehouseClient,
)

from typing import Optional


@mcp.tool()
async def set_warehouse(warehouse: str, ctx: Context) -> str:
    """Set the current warehouse for the session.

    Args:
        warehouse: Name or ID of the warehouse
        ctx: Context object containing client information

    Returns:
        A string confirming the warehouse has been set.
    """
    __ctx_cache[f"{ctx.client_id}_warehouse"] = warehouse
    return f"Warehouse set to '{warehouse}'."


@mcp.tool()
async def list_warehouses(workspace: Optional[str] = None, ctx: Context = None) -> str:
    """List all warehouses in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the list of warehouses or an error message.
    """
    try:
        client = WarehouseClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        warehouses = await client.list_warehouses(
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"]
        )

        return warehouses

    except Exception as e:
        return f"Error listing warehouses: {str(e)}"
