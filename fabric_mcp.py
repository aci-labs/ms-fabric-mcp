from typing import Optional

from mcp.server.fastmcp import FastMCP, Context

from azure.identity import DefaultAzureCredential


from helpers.clients import (
    FabricApiClient,
    LakehouseClient,
    TableClient,
    WarehouseClient,
    WorkspaceClient,
)
from helpers.logging_config import get_logger

from cachetools import TTLCache

logger = get_logger(__name__)

# Create MCP instance with context manager
mcp = FastMCP("fabric_schemas")
mcp.settings.log_level = "debug"

__ctx_cache = TTLCache(maxsize=100, ttl=300)  # Cache for 5 minutes

def get_azure_credentials(client_id: str) -> DefaultAzureCredential:
    """
    Get Azure credentials using DefaultAzureCredential.
    This function is used to authenticate with Azure services.
    """
    __ctx_cache.timer
    if f"{client_id}_creds" in __ctx_cache:
        return __ctx_cache[f"{client_id}_creds"]
    # If credentials are not cached, create a new DefaultAzureCredential instance
    # and store it in the cache.
    else:
        __ctx_cache[f"{client_id}_creds"] = DefaultAzureCredential()
        return __ctx_cache[f"{client_id}_creds"]



@mcp.tool()
async def set_workspace(workspace: str, ctx: Context) -> str:
    """Set the current workspace for the session.

    Args:
        workspace: Name or ID of the workspace
        ctx: Context object containing client information

    Returns:
        A string confirming the workspace has been set.
    """
    __ctx_cache[f"{ctx.client_id}_workspace"] = workspace
    return f"Workspace set to '{workspace}'."


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
async def set_warehouse(warehouse: str, ctx: Context) -> str:
    """Set the current warehouse for the session.

    Args:
        warehouse: Name or ID of the warehouse
        ctx: Context object containing client information

    Returns:
        A string confirming the warehouse has been set.
    """
    __ctx_cache[f"{ctx.client_id}_warehouse"] = warehouse
    return f"Table set to '{warehouse}'."


@mcp.tool()
async def set_table(table_name: str, ctx: Context) -> str:
    """Set the current table for the session.

    Args:
        table_name: Name of the table to set
        ctx: Context object containing client information

    Returns:
        A string confirming the table has been set.
    """
    __ctx_cache[f"{ctx.client_id}_table_name"] = table_name
    return f"Table set to '{table_name}'."


@mcp.tool()
async def clear_context() -> str:
    """Clear the current session context.

    Returns:
        A string confirming the context has been cleared.
    """
    __ctx_cache.clear()
    return "Context cleared."


@mcp.tool()
async def get_lakehouse_table_schema(workspace: Optional[str] , lakehouse: Optional[str] , table_name: str = None, ctx: Context = None) -> str:
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

        if workspace is None:
            if f"{ctx.client_id}_workspace" in __ctx_cache:
                workspace = __ctx_cache[f"{ctx.client_id}_workspace"]
            else:
                return "Workspace must be specified or set in the context."
        
        schema = await client.get_table_schema(workspace, __ctx_cache[f"{ctx.client_id}_lakehouse"] , "lakehouse", table_name, credential)

        return schema

       

    except Exception as e:
        return f"Error retrieving table schema: {str(e)}"


@mcp.tool()
async def get_all_lakehouse_schemas(workspace: Optional[str], lakehouse: Optional[str], ctx: Context) -> str:
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
        schemas = await client.get_all_schemas(workspace, lakehouse, "lakehouse", credential)

        return schemas

    except Exception as e:
        return f"Error retrieving table schemas: {str(e)}"


@mcp.tool()
async def list_workspaces(ctx: Context) -> str:
    """List all available Fabric workspaces.

    Args:
        ctx: Context object containing client information

    Returns:
        A string containing the list of workspaces or an error message.
    """
    try:
        
        client = WorkspaceClient(FabricApiClient(get_azure_credentials(ctx.client_id)))

        workspaces = await client.list_workspaces()

        return workspaces

    except Exception as e:
        return f"Error listing workspaces: {str(e)}"


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

        client = LakehouseClient(FabricApiClient(get_azure_credentials(ctx.client_id)))

        lakehouses = await client.list_lakehouses(workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"])

        return lakehouses
    except Exception as e:
        return f"Error listing lakehouses: {str(e)}"


@mcp.tool()
async def list_warehouses(workspace: Optional[str] = None, ctx: Context= None) -> str:
    """List all warehouses in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the list of warehouses or an error message.
    """
    try:
        
        client = WarehouseClient(FabricApiClient(get_azure_credentials(ctx.client_id)))

        warehouses = await client.list_warehouses(workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"])

        return warehouses

    except Exception as e:
        return f"Error listing warehouses: {str(e)}"


@mcp.tool()
async def list_tables(workspace: Optional[str] = None, lakehouse: Optional[str] = None, warehouse: Optional[str] = None, ctx: Context = None) -> str:
    """List all tables in a Fabric lakehouse or warehouse.

    Args:
        workspace: Name or ID of the workspace (optional)
        lakehouse: Name or ID of the lakehouse (optional)
        warehouse: Name or ID of the warehouse (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the list of tables in markdown format or an error message.
    """
    try:
        client = TableClient(FabricApiClient(get_azure_credentials(ctx.client_id)))
        if lakehouse is None and warehouse is None:
            return "Either lakehouse or warehouse must be specified."
        if lakehouse is not None and warehouse is not None:
            return "Specify either lakehouse or warehouse, not both."
        if warehouse is not None:
            rsc_type = "warehouse"
            rsc_id = warehouse
        else:
            rsc_type = "lakehouse"
            rsc_id = lakehouse

        workspace = workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"]
        tables = await client.list_tables(workspace, rsc_id, type)

        markdown = f"# Tables in {rsc_type} '{rsc_id}'\n\n"
        markdown += "| Name | Format | Type |\n"
        markdown += "|------|--------|------|\n"

        for table in tables:
            markdown += f"| {table['name']} | {table['format']} | {table['type']} |\n"

        return markdown

        
        

    except Exception as e:
        return f"Error listing tables: {str(e)}"


if __name__ == "__main__":
    # Initialize and run the server
    logger.info("Starting MCP server...")

    mcp.run(transport="stdio")