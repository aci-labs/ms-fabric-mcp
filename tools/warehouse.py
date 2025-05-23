from helpers.utils.context import mcp, __ctx_cache
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
    
@mcp.tool()
async def create_warehouse(
    name: str,
    workspace: Optional[str] = None,
    description: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Create a new warehouse in a Fabric workspace.

    Args:
        name: Name of the warehouse
        workspace: Name or ID of the workspace (optional)
        description: Description of the warehouse (optional)
        ctx: Context object containing client information
    Returns:
        A string confirming the warehouse has been created or an error message.
    """
    try:
        client = WarehouseClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        warehouse = await client.create_warehouse(
            name=name,
            workspace=workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"],
            description=description,
        )

        return f"Warehouse '{name}' created successfully."

    except Exception as e:
        return f"Error creating warehouse: {str(e)}"
