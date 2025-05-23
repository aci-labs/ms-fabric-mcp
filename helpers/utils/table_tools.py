from typing import Dict, List, Tuple, Optional
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable
from helpers.logging_config import get_logger
import asyncio

logger = get_logger(__name__)


async def get_delta_schemas(
    tables: List[Dict], credential: DefaultAzureCredential
) -> List[Tuple[Dict, object, object]]:
    """Get schema and metadata for each Delta table"""
    delta_tables = []
    logger.info(f"Starting schema extraction for {len(tables)} tables")

    # Get token for Azure Storage (not Fabric API)
    token = credential.get_token("https://storage.azure.com/.default").token
    storage_options = {"bearer_token": token, "use_fabric_endpoint": "true"}

    for table in tables:
        task = asyncio.create_task(get_delta_table(table, storage_options))
        delta_tables.append(task)
        logger.debug(f"Created task for table: {table['name']}")
    # Wait for all tasks to complete
    delta_tables = await asyncio.gather(*delta_tables)
    logger.info(f"Completed schema extraction for {len(delta_tables)} tables")
    # Filter out None values
    delta_tables = [dt for dt in delta_tables if dt is not None]
    return delta_tables


async def get_delta_table(
    table: Dict, storage_options: Optional[Dict] = None
) -> Optional[Tuple[Dict, object, object]]:
    """Get Delta table schema and metadata"""
    logger.debug(f"Processing table: {table['name']}")

    # Check if the table is a Delta table

    if table["format"].lower() == "delta":
        try:
            table_path = table["location"]
            logger.debug(f"Processing Delta table: {table['name']} at {table_path}")

            # Create DeltaTable instance with storage options
            delta_table = DeltaTable(table_path, storage_options=storage_options)

            # Get both schema and metadata
            result = (table, delta_table.schema(), delta_table.metadata())
            logger.info(f"Processed table: {table['name']}")
            return result

        except Exception as e:
            logger.error(f"Could not process table {table['name']}: {str(e)}")
            return None
