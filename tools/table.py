from helpers.utils.context import mcp, __ctx_cache
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    TableClient,
)

from typing import Optional


@mcp.tool()
async def set_table(table_name: str, ctx: Context) -> str:
    """Set the current table for the session.

    Args:
        table_name: Name of the table to set
        ctx: Context object containing client information

    Returns:
        A string confirming the table has been set.
    """
    __ctx_cache[f"{ctx.client_id}_table"] = table_name
    return f"Table set to '{table_name}'."


@mcp.tool()
async def list_tables(workspace: Optional[str] = None, ctx: Context = None) -> str:
    """List all tables in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the list of tables or an error message.
    """
    try:
        client = TableClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        tables = await client.list_tables(
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"]
        )

        return tables

    except Exception as e:
        return f"Error listing tables: {str(e)}"


@mcp.tool()
async def get_lakehouse_table_schema(
    workspace: Optional[str],
    lakehouse: Optional[str],
    table_name: str = None,
    ctx: Context = None,
) -> str:
    """Get schema for a specific table in a Fabric lakehouse.

    Args:
        workspace: Name or ID of the workspace
        lakehouse: Name or ID of the lakehouse
        table_name: Name of the table to retrieve
        ctx: Context object containing client information

    Returns:
        A string containing the schema of the specified table or an error message.
    """
    try:
        credential = get_azure_credentials(ctx.client_id)
        client = TableClient(FabricApiClient(credential))

        if table_name is None:
            return "Table name must be specified."
        if lakehouse is None:
            if f"{ctx.client_id}_lakehouse" in __ctx_cache:
                lakehouse = __ctx_cache[f"{ctx.client_id}_lakehouse"]
            else:
                return "Lakehouse must be specified or set in the context."

        if workspace is None:
            if f"{ctx.client_id}_workspace" in __ctx_cache:
                workspace = __ctx_cache[f"{ctx.client_id}_workspace"]
            else:
                return "Workspace must be specified or set in the context."

        schema = await client.get_table_schema(
            workspace, lakehouse, "lakehouse", table_name, credential
        )

        return schema

    except Exception as e:
        return f"Error retrieving table schema: {str(e)}"


@mcp.tool()
async def get_all_lakehouse_schemas(
    workspace: Optional[str], lakehouse: Optional[str], ctx: Context
) -> str:
    """Get schemas for all Delta tables in a Fabric lakehouse.

    Args:
        workspace: Name or ID of the workspace
        lakehouse: Name or ID of the lakehouse
        ctx: Context object containing client information

    Returns:
        A string containing the schemas of all Delta tables or an error message.
    """
    try:
        credential = get_azure_credentials(ctx.client_id)
        client = TableClient(FabricApiClient(credential))

        if workspace is None:
            if f"{ctx.client_id}_workspace" in __ctx_cache:
                workspace = __ctx_cache[f"{ctx.client_id}_workspace"]
            else:
                return "Workspace must be specified or set in the context."
        if lakehouse is None:
            if f"{ctx.client_id}_lakehouse" in __ctx_cache:
                lakehouse = __ctx_cache[f"{ctx.client_id}_lakehouse"]
            else:
                return "Lakehouse must be specified or set in the context."
        schemas = await client.get_all_schemas(
            workspace, lakehouse, "lakehouse", credential
        )

        return schemas

    except Exception as e:
        return f"Error retrieving table schemas: {str(e)}"
