from pydantic import BaseModel
from typing import Dict, Any, List, Optional, Tuple, Union
from urllib.parse import quote
from functools import lru_cache
import requests
from azure.identity import DefaultAzureCredential
from helpers.logging_config import get_logger
from helpers.utils import _is_valid_uuid
import json
from uuid import UUID
logger = get_logger(__name__)
from  sempy_labs._helper_functions import create_item

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

    def _build_url(self, endpoint: str, continuation_token: Optional[str] = None) -> str:
        # If the endpoint starts with http, use it as-is.
        url = endpoint if endpoint.startswith("http") else f"{self.config.base_url}/{endpoint.lstrip('/')}"
        if continuation_token:
            separator = "&" if "?" in url else "?"
            encoded_token = quote(continuation_token)
            url += f"{separator}continuationToken={encoded_token}"
        return url

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        method: str = "GET",
        use_pagination: bool = False,
        data_key: str = "value",
        lro: bool = False,  
        lro_poll_interval: int = 2,  # seconds between polls
        lro_timeout: int = 300,  # max seconds to wait
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Make an asynchronous call to the Fabric API.
        
        If use_pagination is True, it will automatically handle paginated responses.
        
        If lro is True, will poll for long-running operation completion.
        """
        import time
        params = params or {}

        # Define a helper to build the full URL from the endpoint.
        

        if not use_pagination:
            url = self._build_url(endpoint=endpoint)
            try:
                if method.upper() == "POST":
                    payload = json.dumps(params)
                    response = requests.post(
                        url,
                        headers=self._get_headers(),
                        json=params,
                        timeout=120,
                    )
                else:
                    if "maxResults" not in params:
                        params["maxResults"] = self.config.max_results
                    logger.debug(f"{method.upper()} {url} with params: {params}")
                    response = requests.request(
                        method=method.upper(),
                        url=url,
                        headers=self._get_headers(),
                        params=params,
                        timeout=120,
                    )
                # LRO support: check for 202 and Operation-Location
                if lro and response.status_code == 202:
                    op_url = response.headers.get("Operation-Location") or response.headers.get("operation-location")
                    if not op_url:
                        logger.error("LRO: No Operation-Location header found.")
                        return None
                    logger.info(f"LRO: Polling {op_url} for operation status...")
                    start_time = time.time()
                    while True:
                        poll_resp = requests.get(op_url, headers=self._get_headers(), timeout=60)
                        if poll_resp.status_code not in (200, 201, 202):
                            logger.error(f"LRO: Poll failed with status {poll_resp.status_code}")
                            return None
                        poll_data = poll_resp.json()
                        status = poll_data.get("status") or poll_data.get("operationStatus")
                        if status in ("Succeeded", "succeeded", "Completed", "completed"):
                            logger.info("LRO: Operation succeeded.")
                            return poll_data
                        if status in ("Failed", "failed", "Canceled", "canceled"):
                            logger.error(f"LRO: Operation failed or canceled. Status: {status}")
                            return poll_data
                        if time.time() - start_time > lro_timeout:
                            logger.error("LRO: Polling timed out.")
                            return None
                        logger.debug(f"LRO: Status {status}, waiting {lro_poll_interval}s...")
                        time.sleep(lro_poll_interval)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logger.error(f"API call failed: {str(e)}")
                if e.response is not None:
                    logger.error(f"Response content: {e.response.text}")
                return None
        else:
            results = []
            continuation_token = None
            while True:
                url = self._build_url(endpoint=endpoint, continuation_token=continuation_token)
                request_params = params.copy()
                # Remove any existing continuationToken in parameters to avoid conflict.
                request_params.pop("continuationToken", None)
                try:
                    if method.upper() == "POST":
                        payload = json.dumps(request_params)
                        logger.debug(f"POST {url} with payload: {payload}")
                        response = requests.post(
                            url,
                            headers=self._get_headers(),
                            json=request_params,
                            timeout=120,
                        )
                    else:
                        if "maxResults" not in request_params:
                            request_params["maxResults"] = self.config.max_results
                        logger.debug(f"{method.upper()} {url} with params: {request_params}")
                        response = requests.request(
                            method=method.upper(),
                            url=url,
                            headers=self._get_headers(),
                            params=request_params,
                            timeout=120,
                        )
                    logger.debug(f"Response: {response.status_code} {response.text}")
                    response.raise_for_status()
                    data = response.json()
                except requests.RequestException as e:
                    logger.error(f"API call failed: {str(e)}")
                    if e.response is not None:
                        logger.error(f"Response content: {e.response.text}")
                    return results if results else None

                if not isinstance(data, dict) or data_key not in data:
                    raise ValueError(f"Unexpected response format: {data}")

                results.extend(data[data_key])
                continuation_token = data.get("continuationToken")
                if not continuation_token:
                    break
            return results

    async def get_workspaces(self) -> List[Dict]:
        """Get all available workspaces"""
        return await self._make_request(
            f"workspaces", use_pagination=True
        )
        return await self.get_items("workspaces")

    async def get_lakehouses(self, workspace_id: str) -> List[Dict]:
        """Get all lakehouses in a workspace"""
        return await self.get_items(
            workspace_id=workspace_id, item_type="Lakehouse"
        )

    async def get_warehouses(self, workspace_id: str) -> List[Dict]:
        """Get all warehouses in a workspace
        Args:
            workspace_id: ID of the workspace
        Returns:
            A list of dictionaries containing warehouse details or an error message.
        """
        return await self.get_items(
            workspace_id=workspace_id, item_type="Warehouse"
        )

    async def get_tables(self, workspace_id: str, rsc_id: str, type: str) -> List[Dict]:
        """Get all tables in a lakehouse
        Args:
            workspace_id: ID of the workspace
            rsc_id: ID of the lakehouse
            type: Type of the resource (e.g., "Lakehouse" or "Warehouse")
        Returns:
            A list of dictionaries containing table details or an error message.
        """
        return await self._make_request(
            f"workspaces/{workspace_id}/{type}s/{rsc_id}/tables",
            use_pagination=True,
            data_key="data",
        )

    async def get_reports(self, workspace_id: str) -> List[Dict]:
        """Get all reports in a lakehouse
        Args:
            workspace_id: ID of the workspace
        Returns:
            A list of dictionaries containing report details or an error message.
        """
        return await self._make_request(
            f"workspaces/{workspace_id}/reports",
            use_pagination=True,
            data_key="value",
        )

    async def get_report(self, workspace_id: str, report_id: str) -> Dict:
        """Get a specific report by ID

        Args:
            workspace_id: ID of the workspace
            report_id: ID of the report

        Returns:
            A dictionary containing the report details or an error message.
        """
        return await self._make_request(
            f"workspaces/{workspace_id}/reports/{report_id}"
        )

    async def get_semantic_models(self, workspace_id: str) -> List[Dict]:
        """Get all semantic models in a lakehouse"""
        return await self._make_request(
            f"workspaces/{workspace_id}/semanticModels",
            use_pagination=True,
            data_key="value",
        )

    async def get_semantic_model(self, workspace_id: str, model_id: str) -> Dict:
        """Get a specific semantic model by ID"""
        return await self._make_request(
            f"workspaces/{workspace_id}/semanticModels/{model_id}"
        )

    async def resolve_workspace(self, workspace: str) -> str:
        """Convert workspace name or ID to workspace ID with caching"""
        return await self._cached_resolve_workspace(workspace)

    async def _resolve_workspace(self, workspace: str) -> str:
        """Internal method to convert workspace name or ID to workspace ID"""
        if _is_valid_uuid(workspace):
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
        if _is_valid_uuid(lakehouse):
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

   
    async def get_items(
        self,
        workspace_id: str,
        item_type: Optional[str] = None,
        params: Optional[Dict] = None
    ) -> List[Dict]:
        """Get all items in a workspace"""
        if not _is_valid_uuid(workspace_id):
            raise ValueError("Invalid workspace ID.")
        if item_type:
            params = params or {}
            params["type"] = item_type
        return await self._make_request(
            f"workspaces/{workspace_id}/items", params=params, use_pagination=True
        )

    async def get_item(
        self,
        item_id: str,
        workspace_id: str,
        item_type: Optional[str] = None,
    ) -> Dict:
        """Get a specific item by ID"""

        if not _is_valid_uuid(item_id):
            item_name, item_id = await self.resolve_item_name_and_id(item_id)
        if not _is_valid_uuid(workspace_id):
            (workspace_name, workspace_id) = await self.resolve_workspace_name_and_id(workspace_id)
        return await self._make_request(
            f"workspaces/{workspace_id}/{item_type}s/{item_id}"
        )

    async def create_item(
            self,
        name: str,
        type: str,
        description: Optional[str] = None,
        definition: Optional[dict] = None,
        workspace: Optional[str | UUID] = None,
    ):
        """
        Creates an item in a Fabric workspace.

        Parameters
        ----------
        name : str
            The name of the item to be created.
        type : str
            The type of the item to be created.
        description : str, default=None
            A description of the item to be created.
        definition : dict, default=None
            The definition of the item to be created.
        workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        """
        from sempy_labs._utils import item_types

        if _is_valid_uuid(workspace):
            workspace_id = workspace
        else:
            (workspace_name, workspace_id) = await self.resolve_workspace_name_and_id(workspace)
        item_type = item_types.get(type)[0].lower()
        item_type_url = item_types.get(type)[1]

        payload = {
            "displayName": name,
            "type": item_type,
        }
        if description:
            payload["description"] = description
        if definition:
            payload["definition"] = definition

        response = await self._make_request(
            endpoint=f"workspaces/{workspace_id}/items",
            method="post",
            params=payload,
            lro=True,
            lro_poll_interval=0.5,
        )
        if response is None:
            raise ValueError(
                f"Failed to create item '{name}' of type '{item_type}' in the '{workspace_id}' workspace."
            )
        if response.get("displayName") != name:
            raise ValueError(
                f"Failed to create item '{name}' of type '{item_type}' in the '{workspace_id}' workspace."
            )
        return response

    async def resolve_item_name_and_id(
            self,
        item: str | UUID, type: Optional[str] = None, workspace: Optional[str | UUID] = None
    ) -> Tuple[str, UUID]:

        (workspace_name, workspace_id) = await self.resolve_workspace_name_and_id(workspace)
        item_id = await self.resolve_item_id(item=item, type=type, workspace=workspace_id)
        item_data = await self._make_request(
            f"workspaces/{workspace_id}/items/{item_id}"
        )
        item_name = item_data.get("displayName")
        return item_name, item_id

    async def resolve_item_id(
            self,
        item: str | UUID, type: Optional[str] = None, workspace: Optional[str | UUID] = None
    ) -> UUID:

        (workspace_name, workspace_id) = await self.resolve_workspace_name_and_id(workspace)
        item_id = None

        if _is_valid_uuid(item):
            # Check (optional)
            item_id = item
            try:
                self._make_request(
                    endpoint=f"workspaces/{workspace_id}/items/{item_id}"
                )
            except requests.RequestException as e:
                raise ValueError(
                    f"The '{item_id}' item was not found in the '{workspace_name}' workspace."
                )
        else:
            if type is None:
                raise ValueError(
                    f"The 'type' parameter is required if specifying an item name."
                )
            responses = await self._make_request(
                endpoint=f"workspaces/{workspace_id}/items?type={type}",
                use_pagination=True,
            )
            # for r in responses:
            for v in responses:
                logger.debug(f"Item: {v}")
                logger.debug(f"Item ID: {v.get('id')}")
                logger.debug(f"Item Type: {v.get('type')} | {v['type']}")
                logger.debug(f"Item Name: {v.get('displayName')} | {v['displayName']}")
                display_name = v['displayName']
                logger.debug(f"Item input: {item}")
                if display_name == item:
                    logger.debug(f"Item found: {display_name}")
                    item_id = v.get("id")
                    break

        if item_id is None:
            raise ValueError(
                f"There's no item '{item}' of type '{type}' in the '{workspace_name}' workspace."
            )

        return item_id
        
    async def resolve_workspace_name_and_id(self, 
        workspace: Optional[str | UUID] = None,
    ) -> Tuple[str, UUID]:
        """
        Obtains the name and ID of the Fabric workspace.

        Parameters
        ----------
        workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.

        Returns
        -------
        str, uuid.UUID
            The name and ID of the Fabric workspace.
        """
        logger.debug(f"Resolving workspace name and ID for: {workspace}")
        if workspace is None:
            raise ValueError("Workspace must be specified.")
        elif _is_valid_uuid(workspace):
            workspace_id = workspace
            workspace_name = await self.resolve_workspace_name(workspace_id)
            return workspace_name, workspace_id
        else:
            responses = await self._make_request(
                endpoint="workspaces", use_pagination=True
            )
            workspace_id = None
            workspace_name = None
            logger.debug(f"-------RESPONSES: {responses}")
            for r in responses:
                display_name = r.get("displayName")
                if display_name == workspace:
                    workspace_name = workspace
                    workspace_id = r.get("id")
                    return workspace_name, workspace_id

        if workspace_name is None or workspace_id is None:
            raise ValueError("Workspace not found")

        return workspace_name, workspace_id
    
    async def resolve_workspace_name(self, workspace_id: Optional[UUID] = None) -> str:

        try:
            response = await self._make_request(
                endpoint=f"workspaces/{workspace_id}"
            )
            logger.debug(f"resolve_workspace_name API response: {response}")
            if not response or "displayName" not in response:
                raise ValueError(f"Workspace '{workspace_id}' not found or API response invalid: {response}")
        except requests.RequestException as e:
            raise ValueError(
                f"The '{workspace_id}' workspace was not found."
            )

        logger.debug(f"Display name of workspace '{workspace_id}': {response.get('displayName')}")
        return response.get("displayName")