from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from urllib.parse import quote
from functools import lru_cache
import requests
from azure.identity import DefaultAzureCredential
from helpers.utils import is_valid_uuid
from helpers.logging_config import get_logger

logger = get_logger(__name__)


class FabricApiConfig(BaseModel):
    """Configuration for Fabric API"""

    base_url: str = "https://api.fabric.microsoft.com/v1"
    max_results: int = 100


class FabricApiClient:
    """Client for communicating with the Fabric API"""

    def __init__(self, credential=None, config=None):
        self.credential = credential or DefaultAzureCredential()
        self.config = config or FabricApiConfig()
        # Initialize cached methods
        self._cached_resolve_workspace = lru_cache(maxsize=128)(self._resolve_workspace)
        self._cached_resolve_lakehouse = lru_cache(maxsize=128)(self._resolve_lakehouse)

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Fabric API calls"""
        return {
            "Authorization": f"Bearer {self.credential.get_token('https://api.fabric.microsoft.com/.default').token}"
        }

    async def _make_request(
        self, endpoint: str, params: Optional[Dict] = None, method: str = "GET"
    ) -> Dict[str, Any]:
        """Make an asynchronous call to the Fabric API"""
        url = (
            endpoint
            if endpoint.startswith("http")
            else f"{self.config.base_url}/{endpoint.lstrip('/')}"
        )
        params = params or {}

        if "maxResults" not in params:
            params["maxResults"] = self.config.max_results

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self._get_headers(),
                params=params,
                timeout=120,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API call failed: {str(e)}")
            return None

    async def paginated_request(
        self, endpoint: str, params: Optional[Dict] = None, data_key: str = "value"
    ) -> List[Dict]:
        """Make a paginated call to the Fabric API"""
        results = []
        params = params or {}
        continuation_token = None

        while True:
            url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
            if continuation_token:
                separator = "&" if "?" in url else "?"
                encoded_token = quote(continuation_token)
                url += f"{separator}continuationToken={encoded_token}"

            request_params = params.copy()
            if "continuationToken" in request_params:
                del request_params["continuationToken"]

            data = await self._make_request(url, request_params)
            if not data:
                break

            if not isinstance(data, dict) or data_key not in data:
                raise ValueError(f"Unexpected response format: {data}")

            results.extend(data[data_key])
            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break

        return results

    async def get_workspaces(self) -> List[Dict]:
        """Get all available workspaces"""
        return await self.paginated_request("workspaces")

    async def get_lakehouses(self, workspace_id: str) -> List[Dict]:
        """Get all lakehouses in a workspace"""
        return await self.paginated_request(
            f"workspaces/{workspace_id}/items", params={"type": "Lakehouse"}
        )

    async def get_warehouses(self, workspace_id: str) -> List[Dict]:
        """Get all warehouses in a workspace"""
        return await self.paginated_request(
            f"workspaces/{workspace_id}/items", params={"type": "Warehouse"}
        )

    async def get_tables(self, workspace_id: str, rsc_id: str, type: str) -> List[Dict]:
        """Get all tables in a lakehouse"""
        return await self.paginated_request(
            f"workspaces/{workspace_id}/{type}s/{rsc_id}/tables",
            data_key="data",
        )
    
    async def get_reports(
        self, workspace_id: str
    ) -> List[Dict]:
        """Get all reports in a lakehouse"""
        return await self.paginated_request(
            f"workspaces/{workspace_id}/reports",
            data_key="value",
        )
    
    async def get_report(
        self, workspace_id: str, report_id: str
    ) -> Dict:
        """Get a specific report by ID"""
        return await self._make_request(
            f"workspaces/{workspace_id}/reports/{report_id}"
        )
    
    async def get_semantic_models(
        self, workspace_id: str
    ) -> List[Dict]:
        """Get all semantic models in a lakehouse"""
        return await self.paginated_request(
            f"workspaces/{workspace_id}/semanticModels",
            data_key="value",
        )
    
    async def get_semantic_model(
        self, workspace_id: str, model_id: str
    ) -> Dict:
        """Get a specific semantic model by ID"""
        return await self._make_request(
            f"workspaces/{workspace_id}/semanticModels/{model_id}"
        )
    
    async def resolve_workspace(self, workspace: str) -> str:
        """Convert workspace name or ID to workspace ID with caching"""
        return await self._cached_resolve_workspace(workspace)

    async def _resolve_workspace(self, workspace: str) -> str:
        """Internal method to convert workspace name or ID to workspace ID"""
        if is_valid_uuid(workspace):
            return workspace

        workspaces = await self.get_workspaces()
        matching_workspaces = [
            w for w in workspaces if w["displayName"].lower() == workspace.lower()
        ]

        if not matching_workspaces:
            raise ValueError(f"No workspaces found with name: {workspace}")
        if len(matching_workspaces) > 1:
            raise ValueError(f"Multiple workspaces found with name: {workspace}")

        return matching_workspaces[0]["id"]

    async def resolve_lakehouse(self, workspace_id: str, lakehouse: str) -> str:
        """Convert lakehouse name or ID to lakehouse ID with caching"""
        return await self._cached_resolve_lakehouse(workspace_id, lakehouse)

    async def _resolve_lakehouse(self, workspace_id: str, lakehouse: str) -> str:
        """Internal method to convert lakehouse name or ID to lakehouse ID"""
        if is_valid_uuid(lakehouse):
            return lakehouse

        lakehouses = await self.get_lakehouses(workspace_id)
        matching_lakehouses = [
            lh for lh in lakehouses if lh["displayName"].lower() == lakehouse.lower()
        ]

        if not matching_lakehouses:
            raise ValueError(f"No lakehouse found with name: {lakehouse}")
        if len(matching_lakehouses) > 1:
            raise ValueError(f"Multiple lakehouses found with name: {lakehouse}")

        return matching_lakehouses[0]["id"]
