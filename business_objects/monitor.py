from typing import Any, List
from . import general
from .. import enums


def get_all_tasks(
    project_id: str = None, only_running: bool = True, limit_per_task: int = 100
) -> List[Any]:
    query = f""" 
    SELECT tasks.*, p.name project_name, orga.name organization_name
    FROM (
        ({__select_running_information_source_payloads(project_id, only_running, limit_per_task)})
        UNION ALL
        ({__select_running_attribute_calculation_tasks(project_id, only_running, limit_per_task)})
        UNION ALL
        ({__select_running_tokenization_tasks(project_id, only_running, limit_per_task)})
        UNION ALL
        ({__select_running_embedding_tasks(project_id, only_running, limit_per_task)})
        UNION ALL
        ({__select_running_weak_supervision_tasks(project_id, only_running, limit_per_task)})
        UNION ALL
        ({__select_running_upload_tasks(project_id, only_running, limit_per_task)})
    ) tasks
    INNER JOIN project p
        ON p.id = tasks.project_id
    INNER JOIN organization orga
        ON orga.id = p.organization_id
    """

    return general.execute_all(query)


def cancel_all_running_tasks(project_id: str = None):
    set_information_source_payloads_to_failed(project_id)
    set_attribute_calculation_to_failed(project_id)
    set_embedding_to_failed(project_id)
    set_record_tokenization_task_to_failed(project_id)
    set_upload_task_to_failed(project_id)
    set_weak_supervision_to_failed(project_id)
    general.commit()


def set_information_source_payloads_to_failed(
    project_id: str = None, payload_id: str = None, with_commit: bool = False
) -> None:
    query = f"""
    UPDATE {enums.Tablenames.INFORMATION_SOURCE_PAYLOAD.value}
    SET state = '{enums.PayloadState.FAILED.value}'
    WHERE state = '{enums.PayloadState.CREATED.value}'
    """

    if project_id:
        query = query + f""" AND project_id = '{project_id}'"""

    if payload_id:
        query = query + f"""AND id = '{payload_id}'"""
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_attribute_calculation_to_failed(
    project_id: str = None, attribute_id: str = None, with_commit: bool = False
) -> None:
    query = f"""
    UPDATE {enums.Tablenames.ATTRIBUTE.value}
    SET state = '{enums.AttributeState.FAILED.value}'
    WHERE state = '{enums.AttributeState.RUNNING.value}'
    """

    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if attribute_id:
        query = query + f"""AND id = '{attribute_id}'"""

    general.execute(query)
    general.flush_or_commit(with_commit)


def set_record_tokenization_task_to_failed(
    project_id: str = None, task_id: str = None, with_commit: bool = False
) -> None:
    query = f"""
    UPDATE {enums.Tablenames.RECORD_TOKENIZATION_TASK.value}
    SET state = '{enums.TokenizerTask.STATE_FAILED.value}'
    WHERE state IN ('{enums.TokenizerTask.STATE_IN_PROGRESS.value}','{enums.TokenizerTask.STATE_CREATED.value}')
    """

    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if task_id:
        query = query + f"""AND id = '{task_id}'"""
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_embedding_to_failed(
    project_id: str = None, embedding_id: str = None, with_commit: bool = False
) -> None:
    query = f"""
    UPDATE {enums.Tablenames.EMBEDDING.value}
    SET state = '{enums.EmbeddingState.FAILED.value}'
    WHERE state IN ('{enums.EmbeddingState.ENCODING.value}','{enums.EmbeddingState.WAITING.value}','{enums.EmbeddingState.INITIALIZING.value}')
    """

    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if embedding_id:
        query = query + f"""AND id = '{embedding_id}'"""
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_weak_supervision_to_failed(
    project_id: str = None, task_id: str = None, with_commit: bool = False
) -> None:
    query = f"""
    UPDATE {enums.Tablenames.WEAK_SUPERVISION_TASK.value}
    SET state = '{enums.PayloadState.FAILED.value}'
    WHERE state IN ('{enums.PayloadState.STARTED.value}','{enums.PayloadState.CREATED.value}')
    """

    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if task_id:
        query = query + f"""AND id = '{task_id}'"""
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_upload_task_to_failed(
    project_id: str = None, task_id: str = None, with_commit: bool = False
) -> None:
    query = f"""
    UPDATE {enums.Tablenames.UPLOAD_TASK.value}
    SET state = '{enums.UploadStates.ERROR.value}'
    WHERE state = '{enums.UploadStates.IN_PROGRESS.value}' OR state = '{enums.UploadStates.PENDING.value}' OR state = '{enums.UploadStates.PREPARED.value}' OR state = '{enums.UploadStates.WAITING.value}'
    """

    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if task_id:
        query = query + f"""AND id = '{task_id}'"""
    general.execute(query)
    general.flush_or_commit(with_commit)


def __select_running_information_source_payloads(
    project_id: str = None, only_running: bool = False, limit_per_task: int = 100
) -> str:
    query = f"""
    SELECT id, 'information_source' task_type, state, project_id, created_by
    FROM {enums.Tablenames.INFORMATION_SOURCE_PAYLOAD.value}
    """
    if project_id and only_running:
        query = (
            query
            + f"""WHERE project_id = '{project_id}' AND state = '{enums.PayloadState.CREATED.value}'"""
        )
    if project_id:
        query = query + f"""WHERE project_id = '{project_id}'"""
    if only_running:
        query = query + f"""WHERE state = '{enums.PayloadState.CREATED.value}'"""
    query = query + f"LIMIT {limit_per_task}"
    return query


def __select_running_attribute_calculation_tasks(
    project_id: str = None, only_running: bool = False, limit: int = 100
) -> str:
    query = f"""
    SELECT id, 'attribute_calculation' task_type, state, project_id, NULL created_by
    FROM {enums.Tablenames.ATTRIBUTE.value}
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.AttributeState.RUNNING.value}'
        """
        )
    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if only_running:
        query = query + f"WHERE state = '{enums.AttributeState.RUNNING.value}'"
    query = query + f"LIMIT {limit}"
    return query


def __select_running_tokenization_tasks(
    project_id: str = None, only_running: bool = False, limit_per_task: int = 100
) -> str:
    query = f"""
    SELECT id, 'tokenization' task_type, state, project_id, user_id created_by
    FROM {enums.Tablenames.RECORD_TOKENIZATION_TASK.value}
    """
    if project_id and only_running:
        query = (
            query
            + f"WHERE project_id = '{project_id}' AND state = '{enums.TokenizerTask.STATE_IN_PROGRESS.value}'"
        )
    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if only_running:
        query = query + f"WHERE state = '{enums.TokenizerTask.STATE_IN_PROGRESS.value}'"
    query = query + f"LIMIT {limit_per_task}"
    return query


def __select_running_embedding_tasks(
    project_id: str = None, only_running: bool = False, limit_per_task: int = 100
) -> str:
    query = f"""
    SELECT id, 'embedding' task_type, state, project_id, NULL created_by
    FROM {enums.Tablenames.EMBEDDING.value}
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state IN ('{enums.EmbeddingState.ENCODING.value}','{enums.EmbeddingState.WAITING.value}','{enums.EmbeddingState.INITIALIZING.value}')
        """
        )
    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if only_running:
        query = (
            query
            + f"""
        WHERE state IN ('{enums.EmbeddingState.ENCODING.value}','{enums.EmbeddingState.WAITING.value}','{enums.EmbeddingState.INITIALIZING.value}')
        """
        )
    query = query + f"LIMIT {limit_per_task}"
    return query


def __select_running_weak_supervision_tasks(
    project_id: str = None, only_running: bool = False, limit_per_task: int = 100
) -> str:
    query = f"""
    SELECT id, 'weak_supervision' task_type, state, project_id, created_by
    FROM {enums.Tablenames.WEAK_SUPERVISION_TASK.value}
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.PayloadState.CREATED.value}'
        """
        )
    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if only_running:
        query = query + f"WHERE state = '{enums.PayloadState.CREATED.value}'"
    query = query + f"LIMIT {limit_per_task}"
    return query


def __select_running_upload_tasks(
    project_id: str = None, only_running: bool = False, limit_per_task: int = 100
) -> str:
    query = f"""
    SELECT id, 'upload' task_type, state, project_id, user_id created_by
    FROM {enums.Tablenames.UPLOAD_TASK.value}
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state IN ('{enums.UploadStates.IN_PROGRESS.value}','{enums.UploadStates.PENDING.value}','{enums.UploadStates.PREPARED.value}','{enums.UploadStates.WAITING.value}')
        """
        )
    if project_id:
        query = query + f"AND project_id = '{project_id}'"
    if only_running:
        query = (
            query
            + f"WHERE state IN ('{enums.UploadStates.IN_PROGRESS.value}','{enums.UploadStates.PENDING.value}','{enums.UploadStates.PREPARED.value}','{enums.UploadStates.WAITING.value}')"
        )
    query = query + f"LIMIT {limit_per_task}"
    return query
