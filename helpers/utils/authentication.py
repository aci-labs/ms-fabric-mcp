from azure.identity import DefaultAzureCredential
from cachetools import TTLCache


def get_azure_credentials(client_id: str, cache: TTLCache) -> DefaultAzureCredential:
    """
    Get Azure credentials using DefaultAzureCredential.
    This function is used to authenticate with Azure services.
    """
    if f"{client_id}_creds" in cache:
        return cache[f"{client_id}_creds"]
    # If credentials are not cached, create a new DefaultAzureCredential instance
    # and store it in the cache.
    else:
        cache[f"{client_id}_creds"] = DefaultAzureCredential()
        return cache[f"{client_id}_creds"]
