from typing import Any, List, Optional
from . import general
from .. import enums
from ..models import TaskQueue, Organization
from ..util import prevent_sql_injection
from ..session import session
from submodules.model.cognition_objects import (
    macro as macro_db_bo,
    markdown_file as markdown_file_db_bo,
    file_extraction as file_extraction_db_bo,
    file_transformation as file_transformation_db_bo,
)

FILE_CACHING_IN_PROGRESS_STATES = [
    enums.FileCachingState.RUNNING.value,
    enums.FileCachingState.CREATED.value,
]


def get_all_tasks(
    page: int = 1,
    limit: int = 100,
) -> List[Any]:

    return (
        session.query(
            TaskQueue.id,
            TaskQueue.task_type,
            TaskQueue.created_at,
            TaskQueue.task_info,
            TaskQueue.created_by,
            TaskQueue.is_active,
            Organization.name,
        )
        .join(Organization, TaskQueue.organization_id == Organization.id)
        .limit(limit)
        .offset(max(0, (page - 1) * limit))
        .all()
    )


def cancel_all_running_tasks(project_id: str = None):
    set_information_source_payloads_to_failed(project_id)
    set_attribute_calculation_to_failed(project_id)
    set_embedding_to_failed(project_id)
    set_record_tokenization_task_to_failed(project_id)
    set_upload_task_to_failed(project_id)
    set_weak_supervision_to_failed(project_id)
    general.commit()


def set_information_source_payloads_to_failed(
    project_id: Optional[str] = None,
    task_id: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
    query = f"""
    UPDATE {enums.Tablenames.INFORMATION_SOURCE_PAYLOAD.value}
    SET state = '{enums.PayloadState.FAILED.value}'
    WHERE state = '{enums.PayloadState.CREATED.value}'
    """
    query = __extend_where_for_update(query, project_id, task_id)
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_attribute_calculation_to_failed(
    project_id: Optional[str] = None,
    task_id: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
    query = f"""
    UPDATE {enums.Tablenames.ATTRIBUTE.value}
    SET state = '{enums.AttributeState.FAILED.value}'
    WHERE state = '{enums.AttributeState.RUNNING.value}'
    """
    query = __extend_where_for_update(query, project_id, task_id)
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_record_tokenization_task_to_failed(
    project_id: Optional[str] = None,
    task_id: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))

    query = f"""
    UPDATE {enums.Tablenames.RECORD_TOKENIZATION_TASK.value}
    SET state = '{enums.TokenizerTask.STATE_FAILED.value}'
    WHERE state IN ('{enums.TokenizerTask.STATE_IN_PROGRESS.value}','{enums.TokenizerTask.STATE_CREATED.value}')
    """
    query = __extend_where_for_update(query, project_id, task_id)
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_embedding_to_failed(
    project_id: Optional[str] = None,
    task_id: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
    query = f"""
    UPDATE {enums.Tablenames.EMBEDDING.value}
    SET state = '{enums.EmbeddingState.FAILED.value}'
    WHERE state IN ('{enums.EmbeddingState.ENCODING.value}','{enums.EmbeddingState.WAITING.value}','{enums.EmbeddingState.INITIALIZING.value}')
    """
    query = __extend_where_for_update(query, project_id, task_id)
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_weak_supervision_to_failed(
    project_id: Optional[str] = None,
    task_id: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
    query = f"""
    UPDATE {enums.Tablenames.WEAK_SUPERVISION_TASK.value}
    SET state = '{enums.PayloadState.FAILED.value}'
    WHERE state IN ('{enums.PayloadState.STARTED.value}','{enums.PayloadState.CREATED.value}')
    """
    query = __extend_where_for_update(query, project_id, task_id)
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_upload_task_to_failed(
    project_id: Optional[str] = None,
    task_id: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    task_id = prevent_sql_injection(task_id, isinstance(task_id, str))
    query = f"""
    UPDATE {enums.Tablenames.UPLOAD_TASK.value}
    SET state = '{enums.UploadStates.ERROR.value}'
    WHERE state = '{enums.UploadStates.IN_PROGRESS.value}' OR state = '{enums.UploadStates.PENDING.value}' OR state = '{enums.UploadStates.PREPARED.value}' OR state = '{enums.UploadStates.WAITING.value}'
    """
    query = __extend_where_for_update(query, project_id, task_id)
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_macro_execution_task_to_failed(
    macro_execution_id: str,
    macro_execution_group_id: str,
    with_commit: bool = False,
) -> None:
    macro_execution = macro_db_bo.get_macro_execution(
        macro_execution_id, macro_execution_group_id
    )
    if macro_execution:
        macro_execution.state = enums.MacroExecutionState.FAILED.value
        general.flush_or_commit(with_commit)


def set_markdown_file_task_to_failed(
    markdown_file_id: str,
    organization_id: str,
    with_commit: bool = False,
) -> None:
    markdown_file = markdown_file_db_bo.get(organization_id, markdown_file_id)
    if markdown_file:
        markdown_file.state = enums.CognitionMarkdownFileState.FAILED.value
        general.flush_or_commit(with_commit)


def set_parse_cognition_file_task_to_failed(
    org_id: str,
    file_reference_id: str,
    extraction_key: str,
    transformation_key: str,
    with_commit: bool = False,
):
    if file_extraction := file_extraction_db_bo.get(
        org_id, file_reference_id, extraction_key
    ):
        if file_extraction.state in FILE_CACHING_IN_PROGRESS_STATES:
            file_extraction.state = enums.FileCachingState.CANCELED.value

        if file_transformation := file_transformation_db_bo.get(
            org_id, file_extraction.id, transformation_key
        ):
            if file_transformation.state in FILE_CACHING_IN_PROGRESS_STATES:
                file_transformation.state = enums.FileCachingState.CANCELED.value
    general.commit()


def __select_running_information_source_payloads(
    project_id: Optional[str] = None,
    only_running: bool = False,
    limit_per_task: int = 100,
) -> str:
    query = f"""
    SELECT id, '{enums.TaskType.INFORMATION_SOURCE.value}' task_type, state, project_id, created_by, created_at AS "started_at", finished_at
    FROM {enums.Tablenames.INFORMATION_SOURCE_PAYLOAD.value}
    """
    only_running_where = (
        f"state = '{enums.PayloadState.CREATED.value}'" if only_running else None
    )
    query += __extend_where_for_select(
        project_id, only_running_where, limit_per_task, "started_at"
    )
    return query


def __select_running_attribute_calculation_tasks(
    project_id: Optional[str] = None,
    only_running: bool = False,
    limit_per_task: int = 100,
) -> str:
    query = f"""
    SELECT id, '{enums.TaskType.ATTRIBUTE_CALCULATION.value}' task_type, state, project_id, NULL created_by, started_at, finished_at
    FROM {enums.Tablenames.ATTRIBUTE.value}
    """
    only_running_where = (
        f"state = '{enums.AttributeState.RUNNING.value}'" if only_running else None
    )
    exclude_uploaded_auto_created = f"state != '{enums.AttributeState.UPLOADED.value}' AND state != '{enums.AttributeState.UPLOADED.value}'"
    query += __extend_where_for_select(
        project_id,
        only_running_where,
        limit_per_task,
        "started_at",
        exclude_uploaded_auto_created,
    )
    return query


def __select_running_tokenization_tasks(
    project_id: Optional[str] = None,
    only_running: bool = False,
    limit_per_task: int = 100,
) -> str:
    query = f"""
    SELECT id, 
    CASE
        WHEN type = '{enums.TokenizerTask.TYPE_DOC_BIN.value}' THEN 'Tokenization - docbins - '
        ELSE 'Tokenization - rats - '
    END ||
    CASE
        WHEN scope = '{enums.TokenizationTaskTypes.PROJECT.value}' THEN 'Project'
        ELSE attribute_name
    END AS task_type, state, project_id, user_id created_by, started_at, finished_at
    FROM {enums.Tablenames.RECORD_TOKENIZATION_TASK.value}
    """
    only_running_where = (
        f"state = '{enums.TokenizerTask.STATE_IN_PROGRESS.value}'"
        if only_running
        else None
    )
    query += __extend_where_for_select(
        project_id, only_running_where, limit_per_task, "started_at"
    )
    return query


def __select_running_embedding_tasks(
    project_id: Optional[str] = None,
    only_running: bool = False,
    limit_per_task: int = 100,
) -> str:
    query = f"""
    SELECT id, '{enums.TaskType.EMBEDDING.value}' task_type, state, project_id, NULL created_by, started_at, finished_at
    FROM {enums.Tablenames.EMBEDDING.value}
    """
    only_running_where = (
        f"state IN ('{enums.EmbeddingState.ENCODING.value}','{enums.EmbeddingState.WAITING.value}','{enums.EmbeddingState.INITIALIZING.value}')"
        if only_running
        else None
    )
    query += __extend_where_for_select(project_id, only_running_where, limit_per_task)
    return query


def __select_running_weak_supervision_tasks(
    project_id: Optional[str] = None,
    only_running: bool = False,
    limit_per_task: int = 100,
) -> str:
    query = f"""
    SELECT id, '{enums.TaskType.WEAK_SUPERVISION.value}' task_type, state, project_id, created_by, created_at as "started_at", finished_at
    FROM {enums.Tablenames.WEAK_SUPERVISION_TASK.value}
    """
    only_running_where = (
        f"state = '{enums.PayloadState.CREATED.value}'" if only_running else None
    )
    query += __extend_where_for_select(
        project_id, only_running_where, limit_per_task, "started_at"
    )
    return query


def __select_running_upload_tasks(
    project_id: Optional[str] = None,
    only_running: bool = False,
    limit_per_task: int = 100,
) -> str:
    query = f"""
    SELECT id, '{enums.TaskType.UPLOAD_TASK.value}' task_type, state, project_id, user_id created_by, started_at, finished_at
    FROM {enums.Tablenames.UPLOAD_TASK.value}
    """

    only_running_where = (
        f"state IN ('{enums.UploadStates.IN_PROGRESS.value}','{enums.UploadStates.PENDING.value}','{enums.UploadStates.PREPARED.value}','{enums.UploadStates.WAITING.value}')"
        if only_running
        else None
    )
    query += __extend_where_for_select(
        project_id, only_running_where, limit_per_task, "started_at"
    )
    return query


def __extend_where_for_update(
    query: str, project_id: Optional[str] = None, task_id: Optional[str] = None
) -> str:
    if project_id:
        query = __extend_where_helper(query, f"project_id = '{project_id}'")
    if task_id:
        query = __extend_where_helper(query, f"id = '{task_id}'")
    return query


def __extend_where_for_select(
    project_id: Optional[str] = None,
    only_running_statement: Optional[str] = None,
    limit_per_task: int = 100,
    started_column: Optional[str] = None,
    exclude_uploaded_auto_created: Optional[str] = None,
):
    query = ""
    if project_id:
        query = __extend_where_helper(query, f"project_id = '{project_id}'")
    if only_running_statement:
        query = __extend_where_helper(
            query,
            only_running_statement,
        )

    if exclude_uploaded_auto_created:
        query = __extend_where_helper(
            query,
            exclude_uploaded_auto_created,
        )

    query = query + f"\nORDER BY {started_column} DESC" if started_column else query
    query += f"\nLIMIT {limit_per_task}"
    return query


def __extend_where_helper(current: str, condition: str) -> str:
    if current:
        return f"{current} AND {condition}"
    else:
        return f" WHERE {condition}"
