from mcp.server.fastmcp import FastMCP
from cachetools import TTLCache

# Create MCP instance with context manager
mcp = FastMCP("fabric_schemas")
mcp.settings.log_level = "debug"

# Shared cache and context
__ctx_cache = TTLCache(maxsize=100, ttl=300)  # Cache for 5 minutes
ctx = mcp.get_context()
