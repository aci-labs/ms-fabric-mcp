from typing import Dict
from helpers.formatters.metadata_formatter import format_metadata_to_markdown


def format_schema_to_markdown(
    table_info: Dict, schema: object, metadata: object
) -> str:
    """Convert a Delta table schema and metadata to a responsive markdown format with HTML."""
    md = f"<h2>Delta Table: <code>{table_info['name']}</code></h2>\n"
    md += f"<p><strong>Type:</strong> {table_info['type']}</p>\n"
    md += f"<p><strong>Location:</strong> <code>{table_info['location']}</code></p>\n\n"

    # Responsive schema table wrapped in a scrollable div
    md += "<h3>Schema</h3>\n"
    md += '<div style="overflow-x:auto;">\n'
    md += '<table style="width:100%; border-collapse: collapse;" border="1">\n'
    md += "  <tr>\n"
    md += "    <th>Column Name</th>\n"
    md += "    <th>Data Type</th>\n"
    md += "    <th>Nullable</th>\n"
    md += "  </tr>\n"

    for field in schema.fields:
        md += "  <tr>\n"
        md += f"    <td>{field.name}</td>\n"
        md += f"    <td>{field.type}</td>\n"
        md += f"    <td>{field.nullable}</td>\n"
        md += "  </tr>\n"

    md += "</table>\n"
    md += "</div>\n\n"

    # Collapsible metadata section for a dynamic feel
    md += "<details>\n"
    md += "  <summary>View Metadata</summary>\n\n"
    md += format_metadata_to_markdown(metadata)
    md += "\n</details>\n"

    return md + "\n"
