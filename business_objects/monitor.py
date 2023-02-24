from . import general
from .. import enums


def get_all_tasks(project_id: str = None, only_running: bool = False):
    query = f"""
    {__select_running_information_source_payloads(project_id, only_running)}
    UNION
    {__select_running_attribute_calculation_tasks(project_id, only_running)}
    UNION
    {__select_running_tokenization_tasks(project_id, only_running)}
    UNION
    {__select_running_embedding_tasks(project_id, only_running)}
    UNION
    {__select_running_weak_supervision_tasks(project_id, only_running)}
    UNION
    {__select_running_upload_tasks(project_id, only_running)}
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
):
    query = f"""
    UPDATE information_source_payload
    SET state = '{enums.PayloadState.FAILED.value}'
    WHERE state = '{enums.PayloadState.STARTED.value}'
    """

    if project_id:
        query = (
            query
            + f"""
        AND project_id = '{project_id}'
        """
        )

    if payload_id:
        query = (
            query
            + f"""
        AND id = '{payload_id}'
        """
        )
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_attribute_calculation_to_failed(
    project_id: str = None, attribute_id: str = None, with_commit: bool = False
):
    query = f"""
    UPDATE attribute
    SET state = '{enums.AttributeState.FAILED.value}'
    WHERE state = '{enums.AttributeState.RUNNING.value}'
    """

    if project_id:
        query = (
            query
            + f"""
        AND project_id = '{project_id}'
        """
        )
    if attribute_id:
        query = (
            query
            + f"""
        AND id = '{attribute_id}'
        """
        )
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_record_tokenization_task_to_failed(
    project_id: str = None, task_id: str = None, with_commit: bool = False
):
    query = f"""
    UPDATE record_tokenization_task
    SET state = '{enums.TokenizerTask.STATE_FAILED.value}'
    WHERE state = '{enums.TokenizerTask.STATE_IN_PROGRESS.value}'
    """

    if project_id:
        query = (
            query
            + f"""
        AND project_id = '{project_id}'
        """
        )
    if task_id:
        query = (
            query
            + f"""
        AND id = '{task_id}'
        """
        )
    print(query)
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_embedding_to_failed(
    project_id: str = None, embedding_id: str = None, with_commit: bool = False
):
    query = f"""
    UPDATE embedding
    SET state = '{enums.EmbeddingState.FAILED.value}'
    WHERE state = '{enums.EmbeddingState.ENCODING.value}' OR state = '{enums.EmbeddingState.WAITING.value}' OR state = '{enums.EmbeddingState.INITIALIZING.value}'
    """

    if project_id:
        query = (
            query
            + f"""
        AND project_id = '{project_id}'
        """
        )
    if embedding_id:
        query = (
            query
            + f"""
        AND id = '{embedding_id}'
        """
        )
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_weak_supervision_to_failed(
    project_id: str = None, task_id: str = None, with_commit: bool = False
):
    query = f"""
    UPDATE weak_supervision_task
    SET state = '{enums.PayloadState.FAILED.value}'
    WHERE state = '{enums.PayloadState.STARTED.value}'
    """

    if project_id:
        query = (
            query
            + f"""
        AND project_id = '{project_id}'
        """
        )
    if task_id:
        query = (
            query
            + f"""
        AND id = '{task_id}'
        """
        )
    general.execute(query)
    general.flush_or_commit(with_commit)


def set_upload_task_to_failed(
    project_id: str = None, task_id: str = None, with_commit: bool = False
):
    query = f"""
    UPDATE upload_task
    SET state = '{enums.UploadStates.ERROR.value}'
    WHERE state = '{enums.UploadStates.IN_PROGRESS.value}' OR state = '{enums.UploadStates.PENDING.value}' OR state = '{enums.UploadStates.PREPARED.value}' OR state = '{enums.UploadStates.WAITING.value}'
    """

    if project_id:
        query = (
            query
            + f"""
        AND project_id = '{project_id}'
        """
        )
    if task_id:
        query = (
            query
            + f"""
        AND id = '{task_id}'
        """
        )
    general.execute(query)
    general.flush_or_commit(with_commit)


def __select_running_information_source_payloads(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'information_source' as task_type
    FROM information_source_payload
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.PayloadState.CREATED.value}'
        """
        )
    if project_id:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}'
        """
        )
    if only_running:
        query = (
            query
            + f"""
        WHERE state = '{enums.PayloadState.CREATED.value}'
        """
        )
    return query


def __select_running_attribute_calculation_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'attribute_calculation' as task_type
    FROM attribute
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.AttributeState.RUNNING.value}'
        """
        )
    if project_id:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}'
        """
        )
    if only_running:
        query = (
            query
            + f"""
        WHERE state = '{enums.AttributeState.RUNNING.value}'
        """
        )
    return query


def __select_running_tokenization_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'tokenization' as task_type
    FROM record_tokenization_task
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.TokenizerTask.STATE_IN_PROGRESS.value}' 
        """
        )
    if project_id:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}'
        """
        )
    if only_running:
        query = (
            query
            + f"""
        WHERE state = '{enums.TokenizerTask.STATE_IN_PROGRESS.value}'
        """
        )
    return query


def __select_running_embedding_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'embedding' as task_type
    FROM embedding
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND (state = '{enums.EmbeddingState.ENCODING.value}' OR state = '{enums.EmbeddingState.WAITING.value}' OR state = '{enums.EmbeddingState.INITIALIZING.value}')
        """
        )
    if project_id:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}'
        """
        )
    if only_running:
        query = (
            query
            + f"""
        WHERE state = '{enums.EmbeddingState.ENCODING.value}' OR state = '{enums.EmbeddingState.WAITING.value}' OR state = '{enums.EmbeddingState.INITIALIZING.value}'
        """
        )
    return query


def __select_running_weak_supervision_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'weak_supervision' as task_type
    FROM weak_supervision_task
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.PayloadState.CREATED.value}'
        """
        )
    if project_id:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}'
        """
        )
    if only_running:
        query = (
            query
            + f"""
        WHERE state = '{enums.PayloadState.CREATED.value}'
        """
        )
    return query


def __select_running_upload_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'upload' as task_type
    FROM upload_task
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND (state = '{enums.UploadStates.IN_PROGRESS.value}' OR state = '{enums.UploadStates.PENDING.value}' OR state = '{enums.UploadStates.PREPARED.value}' OR state = '{enums.UploadStates.WAITING.value}')
        """
        )
    if project_id:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}'
        """
        )
    if only_running:
        query = (
            query
            + f"""
        WHERE state = '{enums.UploadStates.IN_PROGRESS.value}' OR state = '{enums.UploadStates.PENDING.value}' OR state = '{enums.UploadStates.PREPARED.value}' OR state = '{enums.UploadStates.WAITING.value}'
        """
        )
    return query
