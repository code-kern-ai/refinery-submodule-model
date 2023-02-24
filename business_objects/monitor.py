from . import general
from .. import enums


def get_all_tasks(project_id: None, only_running: False):
    query = f"""
    {get_running_information_source_payloads(project_id, only_running)}
    UNION
    {get_running_attribute_calculation_tasks(project_id, only_running)}
    UNION
    {get_running_tokenization_tasks(project_id, only_running)}
    UNION
    {get_running_embedding_tasks(project_id, only_running)}
    UNION
    {get_running_weak_supervision_tasks(project_id, only_running)}
    UNION
    {get_running_upload_tasks(project_id, only_running)}
    """
    return general.execute_all(query)


def get_running_information_source_payloads(project_id: None, only_running: False):
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


def get_running_attribute_calculation_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'attribute_calculation' as task_type
    FROM weak_supervision_task
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


def get_running_tokenization_tasks(project_id: None, only_running: False):
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


def get_running_embedding_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'embedding' as task_type
    FROM embedding
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.EmbeddingState.ENCODING.value}'
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
        WHERE state = '{enums.EmbeddingState.ENCODING.value}'
        """
        )
    return query


def get_running_weak_supervision_tasks(project_id: None, only_running: False):
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


def get_running_upload_tasks(project_id: None, only_running: False):
    query = f"""
    SELECT id, 'upload' as task_type
    FROM upload_task
    """
    if project_id and only_running:
        query = (
            query
            + f"""
        WHERE project_id = '{project_id}' AND state = '{enums.UploadStates.IN_PROGRESS.value}'
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
        WHERE state = '{enums.UploadStates.IN_PROGRESS.value}'
        """
        )
    return query
