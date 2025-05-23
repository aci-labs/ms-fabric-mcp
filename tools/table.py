from helpers.utils.context import mcp, __ctx_cache
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    TableClient,
    SQLClient,
    get_sql_endpoint,
)

from typing import Optional
from helpers.logging_config import get_logger

logger = get_logger(__name__)


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
async def list_tables(workspace: Optional[str] = None, lakehouse: Optional[str] = None, ctx: Context = None) -> str:
    """List all tables in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        lakehouse: Name or ID of the lakehouse (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the list of tables or an error message.
    """
    try:
        client = TableClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        tables = await client.list_tables(
            workspace_id=workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"],
            rsc_id=lakehouse if lakehouse else __ctx_cache[f"{ctx.client_id}_lakehouse"]
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
        credential = get_azure_credentials(ctx.client_id, __ctx_cache)
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
        credential = get_azure_credentials(ctx.client_id, __ctx_cache)
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


@mcp.tool()
async def read_table( 
    workspace: Optional[str] = None,
    lakehouse: Optional[str] = None,
    warehouse: Optional[str] = None,
    table_name: str = None,
    type: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Read data from a table in a warehouse or lakehouse.

    Args:
        workspace: Name or ID of the workspace (optional).
        lakehouse: Name or ID of the lakehouse (optional).
        warehouse: Name or ID of the warehouse (optional).
        table_name: The name of the table to read data from.
        type: Type of resource ('lakehouse' or 'warehouse').
        ctx: Context object containing client information.
    Returns:
        A string confirming the data read or an error message.
    """
    try:
        if ctx is None:
            raise ValueError("Context (ctx) must be provided.")
        if table_name is None:
            raise ValueError("Table name must be specified.")
        # Always resolve the SQL endpoint and database name
        database, sql_endpoint = await get_sql_endpoint(
            workspace=workspace,
            lakehouse=lakehouse,
            warehouse=warehouse,
            type=type,
        )
        if not database or not sql_endpoint or sql_endpoint.startswith("Error") or sql_endpoint.startswith("No SQL endpoint"):
            return f"Failed to resolve SQL endpoint: {sql_endpoint}"
        logger.info(f"Reading table {table_name} from SQL endpoint {sql_endpoint}")
        client = SQLClient(sql_endpoint=sql_endpoint, database=database)
        df = client.read_table(table_name)
        if df.is_empty():
            return f"No data found in table '{table_name}'."
        # Convert to markdown for user-friendly display
        markdown = f"### Table: {table_name} (shape: {df.shape})\n\n"
        markdown += df.head(10).to_pandas().to_markdown(index=False)
        markdown += f"\n\nColumns: {', '.join(df.columns)}"
        return markdown
    except Exception as e:
        logger.error(f"Error reading data: {str(e)}")
        return f"Error reading data: {str(e)}"