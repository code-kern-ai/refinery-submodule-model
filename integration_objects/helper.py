from typing import Set

from ..enums import Tablenames


REFINERY_ATTRIBUTE_ACCESS_GROUPS = "<ACCESS_GROUPS>"
REFINERY_ATTRIBUTE_ACCESS_USERS = "<ACCESS_USERS>"

DEFAULT_METADATA = {"source", "minio_file_name", "running_ids"}
TABLE_METADATA = {
    Tablenames.INTEGRATION_PDF.value: {"file_path", "page", "total_pages", "title"},
    Tablenames.INTEGRATION_GITHUB_FILE.value: {"path", "sha", "code_language"},
    Tablenames.INTEGRATION_GITHUB_ISSUE.value: {
        "url",
        "state",
        "number",
        "assignee",
        "milestone",
    },
    Tablenames.INTEGRATION_SHAREPOINT.value: {
        "extension",
        "object_id",
        "parent_path",
        "name",
        "web_url",
        f"{Tablenames.INTEGRATION_SHAREPOINT.value}_created_by",
        "modified_by",
        "created",
        "modified",
        "description",
        "size",
        "mime_type",
        "hashes",
        "permissions",
        "file_properties",
    },
    Tablenames.INTEGRATION_WEBPAGE.value: {"raw_markdown_content", "title"},
}


def get_supported_metadata_keys(table_name: str) -> Set[str]:
    """
    Function for controlling and documenting the dynamic metadata fields associated with different integration types.

    The `TABLE_METADATA` dictionary defines which metadata keys are expected and allowed for each integration table
    (e.g., `integration.sharepoint`, `integration.github_file`). Each value contains a set of keys specific to that integration, while
    the `DEFAULT_METADATA` (`source`, `running_id`, `minio_file_name`) are always included.

    During extraction, metadata is dynamically attached to each document according to the rules defined here.

    This function is used by the integration object logic (see `src.util.integration #delta_load`) to
    validate and filter metadata before persisting it, ensuring consistency and preventing unwanted fields from being
    stored in the database.

    Example:
        get_supported_metadata("pdf")
        # returns: {"source", "minio_file_name", "running_id", "file_path", "page", "total_pages", "title"}
    """
    return DEFAULT_METADATA.union(TABLE_METADATA.get(table_name, set()))
