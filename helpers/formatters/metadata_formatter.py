from datetime import datetime
import json


def format_metadata_to_markdown(metadata: object) -> str:
    """Convert Delta table metadata to a responsive markdown format with HTML."""
    md = "#### Metadata\n\n"
    md += "<dl>\n"
    md += f"  <dt>ID:</dt><dd>{metadata.id}</dd>\n"
    if metadata.name:
        md += f"  <dt>Name:</dt><dd>{metadata.name}</dd>\n"
    if metadata.description:
        md += f"  <dt>Description:</dt><dd>{metadata.description}</dd>\n"
    if metadata.partition_columns:
        md += f"  <dt>Partition Columns:</dt><dd>{', '.join(metadata.partition_columns)}</dd>\n"
    if metadata.created_time:
        created_time = datetime.fromtimestamp(metadata.created_time / 1000)
        md += f"  <dt>Created:</dt><dd>{created_time.strftime('%Y-%m-%d %H:%M:%S')}</dd>\n"
    if metadata.configuration:
        md += "  <dt>Configuration:</dt>\n"
        md += "  <dd>\n"
        md += "    <details>\n"
        md += "      <summary>View JSON</summary>\n"
        md += "      <pre><code>\n"
        md += json.dumps(metadata.configuration, indent=2)
        md += "\n      </code></pre>\n"
        md += "    </details>\n"
        md += "  </dd>\n"
    md += "</dl>\n"
    return md
