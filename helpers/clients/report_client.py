from helpers.logging_config import get_logger
from helpers.clients.fabric_client import FabricApiClient

logger = get_logger(__name__)

class ReportClient:
    def __init__(self, client: FabricApiClient):
        self.client = client

    async def list_reports(self, workspace_id: str):
        """List all reports in a workspace."""
        reports = await self.client.get_reports(workspace_id)

        if not reports:
            return f"No reports found in workspace '{workspace_id}'."

        return reports
    
    async def get_report(
        self, workspace_id: str, report_id: str
    ) -> dict:
        """Get a specific report by ID."""
        report = await self.client.get_report(workspace_id, report_id)

        if not report:
            return f"No report found with ID '{report_id}' in workspace '{workspace_id}'."

        return report