import polars as pl
from sqlalchemy import create_engine, Engine
from itertools import chain, repeat
import urllib
import struct
from typing import Optional
from azure.identity import DefaultAzureCredential
from helpers.clients import FabricApiClient, LakehouseClient, WarehouseClient


# prepare connection string
sql_endpoint = "lkxke5qat5vu7fpnluz5o7cnme-qlbrb7caj77uthvfhqdxwd5v54.datawarehouse.fabric.microsoft.com"
database = "EDR_WAREHOUSE"
DRIVER = "{{ODBC Driver 18 for SQL Server}}"


def get_sqlalchemy_connection_string(driver: str, server: str, database: str) -> Engine:
    """
    Constructs a SQLAlchemy connection string based on the provided parameters.

    Args:
        driver (str): The database driver (e.g., 'mssql+pyodbc').
        server (str): The server address.
        database (str): The database name.

    Returns:
        Engine: A SQLAlchemy engine object.
    """
    connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_endpoint},1433;Database={database};Encrypt=Yes;TrustServerCertificate=No"
    params = urllib.parse.quote(connection_string)
    # authentication
    resource_url = "https://database.windows.net/.default"
    azure_credentials = DefaultAzureCredential()
    token_object = azure_credentials.get_token(resource_url)
    # Retrieve an access token
    token_as_bytes = bytes(
        token_object.token, "UTF-8"
    )  # Convert the token to a UTF-8 byte string
    encoded_bytes = bytes(
        chain.from_iterable(zip(token_as_bytes, repeat(0)))
    )  # Encode the bytes to a Windows byte string
    token_bytes = (
        struct.pack("<i", len(encoded_bytes)) + encoded_bytes
    )  # Package the token into a bytes object
    attrs_before = {
        1256: token_bytes
    }  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

    # build the connection
    engine = create_engine(
        "mssql+pyodbc:///?odbc_connect={0}".format(params),
        connect_args={"attrs_before": attrs_before},
    )
    return engine


async def get_sql_endpoint(
    workspace: str = None,
    lakehouse: Optional[str] = None,
    warehouse: Optional[str] = None,
    type: str = None,
) -> tuple:
    """
    Retrieve the SQL endpoint for a specified lakehouse or warehouse.

    Args:
        lakehouse: Name or ID of the lakehouse (optional).
        warehouse: Name or ID of the warehouse (optional).
        type: Type of resource ('lakehouse' or 'warehouse').
        workspace: Name or ID of the workspace (optional).
    Returns:
        A tuple (database, sql_endpoint) or (None, error_message) in case of error.
    """
    try:
        credential = DefaultAzureCredential()
        fabClient = FabricApiClient(credential)
        resource_name = None
        endpoint = None
        workspace_name, workspace_id = await fabClient.resolve_workspace_name_and_id(
            workspace
        )
        if type and type.lower() == "lakehouse":
            client = LakehouseClient(fabClient)
            resource_name, resource_id = await fabClient.resolve_item_name_and_id(
                workspace=workspace_id, item=lakehouse, type="Lakehouse"
            )
            lakehouse_obj = await client.get_lakehouse(
                workspace=workspace, lakehouse=resource_id
            )
            endpoint = (
                lakehouse_obj.get("properties", {})
                .get("sqlEndpointProperties", {})
                .get("connectionString")
            )
        elif type and type.lower() == "warehouse":
            client = WarehouseClient(fabClient)
            resource_name, resource_id = await fabClient.resolve_item_name_and_id(
                workspace=workspace_id, item=warehouse, type="Warehouse"
            )
            warehouse_obj = await client.get_warehouse(
                workspace=workspace, warehouse=resource_id
            )
            endpoint = warehouse_obj.get("properties", {}).get("connectionString")
        if resource_name and endpoint:
            return resource_name, endpoint
        else:
            return (
                None,
                f"No SQL endpoint found for {type} '{lakehouse or warehouse}' in workspace '{workspace}'.",
            )
    except Exception as e:
        return None, f"Error retrieving SQL endpoint: {str(e)}"


class SQLClient:
    def __init__(self, sql_endpoint: str, database: str):
        self.engine = get_sqlalchemy_connection_string(DRIVER, sql_endpoint, database)

    def run_query(self, query: str) -> pl.DataFrame:
        return pl.read_database(query, connection=self.engine)

    def load_data(self, df: pl.DataFrame, table_name: str, if_exists: str = "append"):
        pdf = df.to_pandas()
        pdf.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
