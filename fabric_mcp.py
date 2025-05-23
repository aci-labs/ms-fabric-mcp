from tools import *
from helpers.logging_config import get_logger
from helpers.utils.context import mcp, __ctx_cache

logger = get_logger(__name__)


@mcp.tool()
async def clear_context() -> str:
    """Clear the current session context.

    Returns:
        A string confirming the context has been cleared.
    """
    __ctx_cache.clear()
    return "Context cleared."


if __name__ == "__main__":
    # Initialize and run the server
    logger.info("Starting MCP server...")

    mcp.run(transport="stdio")
