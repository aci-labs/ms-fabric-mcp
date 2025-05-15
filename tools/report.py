from helpers.utils.context import mcp, __ctx_cache
from mcp.server.fastmcp import Context
from helpers.utils.authentication import get_azure_credentials
from helpers.clients import (
    FabricApiClient,
    ReportClient,
)
from helpers.logging_config import get_logger
from typing import Optional

logger = get_logger(__name__)


@mcp.tool()
async def list_reports(workspace: Optional[str] = None, ctx: Context = None) -> str:
    """List all reports in a Fabric workspace.

    Args:
        workspace: Name or ID of the workspace (optional)
        ctx: Context object containing client information
    Returns:
        A string containing the list of reports or an error message.
    """
    try:
        client = ReportClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        reports = await client.list_reports(
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"]
        )

        markdown = f"# Reports in workspace '{workspace}'\n\n"
        markdown += "| ID | Name | Description |\n"
        markdown += "|-----|------|-------------|\n"

        for report in reports:
            markdown += f"| {report.get('id', 'N/A')} | {report.get('displayName', 'N/A')} | {report.get('description', 'N/A')} |\n"

        return markdown

    except Exception as e:
        return f"Error listing reports: {str(e)}"


@mcp.tool()
async def get_report(
    workspace: Optional[str] = None,
    report_id: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Get a specific report by ID.

    Args:
        workspace: Name or ID of the workspace (optional)
        report_id: ID of the report (optional)
        ctx: Context object containing client information

    Returns:
        A string containing the report details or an error message.
    """
    try:
        client = ReportClient(
            FabricApiClient(get_azure_credentials(ctx.client_id, __ctx_cache))
        )

        report = await client.get_report(
            workspace if workspace else __ctx_cache[f"{ctx.client_id}_workspace"],
            report_id,
        )

        if not report:
            return f"No report found with ID '{report_id}' in workspace '{workspace}'."

        return f"Report details:\n\n{report}"

    except Exception as e:
        return f"Error getting report: {str(e)}"
