from typing import Optional
from helpers.utils.context import mcp, __ctx_cache
from mcp.server.fastmcp import Context
from helpers.clients import get_sql_endpoint


@mcp.tool()
async def get_sql_endpoint(
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    warehouse: Optional[str] = None,
    type: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """
    Retrieve the SQL endpoint for a specified lakehouse or warehouse.

    Args:
        workspace: Name or ID of the workspace (optional).
        lakehouse: Name or ID of the lakehouse (optional).
        warehouse: Name or ID of the warehouse (optional).
        type: Type of resource ('lakehouse' or 'warehouse'). If not provided, it will be inferred.
        ctx: Context object containing client information.

    Returns:
        A string containing the resource type, name/ID, and its SQL endpoint.
    """
    try:
        if ctx is None:
            raise ValueError("Context (ctx) must be provided.")

        if workspace is None:
            workspace = __ctx_cache.get(f"{ctx.client_id}_workspace")
            if workspace is None:
                raise ValueError("Workspace must be specified or set in context.")
        if lakehouse is None and warehouse is None:
            lakehouse = __ctx_cache.get(f"{ctx.client_id}_lakehouse")
            warehouse = __ctx_cache.get(f"{ctx.client_id}_warehouse")
            if warehouse is None and lakehouse is None:
                raise ValueError(
                    "Either lakehouse or warehouse must be specified or set in context."
                )

        name, endpoint = await get_sql_endpoint(
            workspace=workspace,
            lakehouse=lakehouse,
            warehouse=warehouse,  # Add warehouse to the call
            type=type,
        )

        return (
            endpoint
            if endpoint
            else f"No SQL endpoint found for {type} '{lakehouse or warehouse}' in workspace '{workspace}'."
        )
    except Exception as e:
        return f"Error retrieving SQL endpoint: {str(e)}"
