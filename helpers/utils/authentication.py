from azure.identity import DefaultAzureCredential
from fabric_mcp import __ctx_cache


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