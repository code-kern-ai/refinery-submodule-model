from datetime import datetime
from typing import List, Optional

from . import general
from .. import enums
from ..models import InformationSource, InformationSourcePayload
from ..session import session


def get(project_id: str, payload_id: str) -> InformationSourcePayload:
    return (
        session.query(InformationSourcePayload)
        .filter(
            InformationSourcePayload.project_id == project_id,
            InformationSourcePayload.id == payload_id,
        )
        .first()
    )


def get_max_token(record_ids: List[str], labeling_task_id: str, project_id: str):
    query = get_query_max_token(project_id, labeling_task_id, record_ids)
    return {x.record_id: x.num_token for x in general.execute_all(query)}


def get_query_labels_classification(
    project_id: str, labeling_task_id: str, source_type: str
) -> str:
    query: str = f"""
    SELECT embeddings.record_id, labels.name
    FROM (
        SELECT DISTINCT et.record_id
        FROM embedding e
        INNER JOIN embedding_tensor et
        ON e.id = et.embedding_id
        WHERE e.project_id = '{project_id}'
    ) embeddings
    LEFT JOIN (
        SELECT rla.record_id, label.name
        FROM record_label_association rla
        INNER JOIN labeling_task_label label
        ON rla.labeling_task_label_id = label.id
        INNER JOIN labeling_task task
        ON label.labeling_task_id = task.id
        WHERE rla.source_type = '{source_type}'
        AND task.id = '{labeling_task_id}'
    ) labels
    ON embeddings.record_id = labels.record_id
    ORDER BY embeddings.record_id
    """
    return query


def get_query_labels_classification_manual(
    project_id: str, labeling_task_id: str
) -> str:
    # gold /gold star + all where all agree need to be found so a bit more complicated
    base_query: str = get_base_query_valid_labels_manual(project_id, labeling_task_id)
    query: str = (
        base_query
        + f"""
    SELECT e.record_id,rla.name
    FROM (
        SELECT record_id 
        FROM embedding e
        INNER JOIN embedding_tensor et
            ON e.id = et.embedding_id
        WHERE e.project_id = '{project_id}'
        GROUP BY record_id )e
    LEFT JOIN (
        SELECT rla.record_id, ltl.name
        FROM valid_rla_ids vri
        INNER JOIN record_label_association rla
            ON rla.id = vri.rla_id
        INNER JOIN labeling_task_label ltl
            ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id 
            AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}'
        WHERE rla.source_type = '{enums.LabelSource.MANUAL.value}' 
            AND ltl.labeling_task_id = '{labeling_task_id}' ) rla
    ON e.record_id = rla.record_id
    ORDER BY e.record_id
    """
    )
    return query


def get_query_labels_extraction(
    project_id: str, labeling_task_id: str, source_type: str
) -> str:
    query: str = f"""
    SELECT embeddings.record_id::text, labels.name, labels.token_list
    FROM (
        SELECT DISTINCT et.record_id
        FROM embedding e
        INNER JOIN embedding_tensor et
        ON e.id = et.embedding_id
        WHERE e.project_id = '{project_id}'
    ) embeddings
    LEFT JOIN (
        SELECT rla.record_id, label.name, array_agg(token.token_index) AS token_list
        FROM record_label_association rla
        INNER JOIN record_label_association_token token
        ON rla.id = token.record_label_association_id
        INNER JOIN labeling_task_label label
        ON rla.labeling_task_label_id = label.id
        INNER JOIN labeling_task task
        ON label.labeling_task_id = task.id
        WHERE rla.source_type = '{source_type}'
        AND task.id = '{labeling_task_id}'
        GROUP BY rla.record_id, label.name
    ) labels
    ON embeddings.record_id = labels.record_id
    ORDER BY embeddings.record_id ASC
    """
    return query


def get_query_labels_extraction_manual(project_id: str, labeling_task_id: str) -> str:
    # gold /gold star + all where all agree need to be found so a bit more complicated
    base_query: str = get_base_query_valid_labels_manual(project_id, labeling_task_id)
    query: str = (
        base_query
        + f"""
    SELECT embeddings.record_id::text, labels.name, labels.token_list
    FROM (
        SELECT et.record_id
        FROM embedding e
        INNER JOIN embedding_tensor et
        ON e.id = et.embedding_id
        WHERE e.project_id = '{project_id}'
        GROUP BY et.record_id
    ) embeddings
    LEFT JOIN (
        SELECT rla.record_id, ltl.name, array_agg(token.token_index) AS token_list
        FROM valid_rla_ids vri
        INNER JOIN record_label_association rla
        ON vri.rla_id = rla.id
        INNER JOIN record_label_association_token token
        ON rla.id = token.record_label_association_id
        INNER JOIN labeling_task_label ltl
        ON rla.labeling_task_label_id = ltl.id
        WHERE rla.source_type = '{enums.LabelSource.MANUAL.value}'
        AND ltl.labeling_task_id = '{labeling_task_id}'
        GROUP BY rla.record_id, ltl.name
    ) labels
    ON embeddings.record_id = labels.record_id
    ORDER BY embeddings.record_id ASC
    """
    )
    return query


def get_base_query_valid_labels_manual(
    project_id: str, labeling_task_id: str = "", record_id: str = ""
) -> str:
    # is_valid_manual_label is now set on rla creation
    labeling_task_add = ""
    if labeling_task_id:
        labeling_task_add = f"AND ltl.labeling_task_id = '{labeling_task_id}'"
    record_id_add = ""
    if record_id:
        record_id_add = f"AND rla.record_id = '{record_id}'"
    query = f"""
    WITH valid_rla_ids AS(    
        SELECT rla.id rla_id
        FROM record_label_association rla
        INNER JOIN labeling_task_label ltl
            ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id 
        WHERE rla.is_valid_manual_label = TRUE 
            AND rla.source_type = '{enums.LabelSource.MANUAL.value}' 
            AND rla.project_id = '{project_id}' AND ltl.project_id = '{project_id}' 
        {labeling_task_add} 
        {record_id_add} 
	)
    """
    return query


def get_query_max_token(
    project_id: str, labeling_task_id: str, record_ids: List[str]
) -> str:
    if not isinstance(record_ids, str):
        record_ids = "'" + "','".join(record_ids) + "'"

    query = f"""
    SELECT rats.record_id::TEXT record_id, rats.num_token
    FROM record_attribute_token_statistics rats
    INNER JOIN attribute a 
        ON rats.attribute_id = a.id
    INNER JOIN labeling_task lt
        ON a.id = lt.attribute_id
    WHERE lt.id = '{labeling_task_id}' AND a.project_id = '{project_id}'
    AND rats.record_id IN ({record_ids})
    GROUP BY rats.record_id, rats.num_token
    """
    return query


def update_progress(
    project_id: str, payload_id: str, progress: float, with_commit: bool = True
):
    payload = (
        session.query(InformationSourcePayload)
        .filter(
            InformationSourcePayload.project_id == project_id,
            InformationSourcePayload.id == payload_id,
        )
        .first()
    )

    if payload is not None:
        payload.progress = progress
        general.flush_or_commit(with_commit)


def update_status(
    project_id: str, payload_id: str, status: str, with_commit: bool = True
):
    payload = (
        session.query(InformationSourcePayload)
        .filter(
            InformationSourcePayload.project_id == project_id,
            InformationSourcePayload.id == payload_id,
        )
        .first()
    )

    if payload is not None:
        payload.state = status
        general.flush_or_commit(with_commit)


def create(
    project_id: str,
    source_code: str,
    state: enums.PayloadState,
    iteration: int,
    source_id: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> InformationSourcePayload:
    payload: InformationSourcePayload = InformationSourcePayload(
        source_code=source_code,
        state=state.value,
        iteration=iteration,
        source_id=source_id,
        created_by=created_by,
        project_id=project_id,
        created_at=created_at,
    )
    general.add(payload, with_commit)
    return payload


def create_empty_crowd_payload(
    project_id: str, information_source_id: str, user_id: str
) -> InformationSourcePayload:
    return create(
        project_id=project_id,
        source_code="",  # empty payload
        state=enums.PayloadState.STARTED,
        iteration=0,
        source_id=information_source_id,
        created_by=user_id,
    )


def remove(
    project_id: str, source_id: str, payload_id: str, with_commit: bool = False
) -> None:
    item = get(project_id, payload_id)
    if not item:
        raise ValueError(f"Payload {payload_id} not found")
    if str(item.source_id) == source_id:
        session.delete(item)
        general.flush_or_commit(with_commit)
    else:
        raise ValueError("Payload does not belong to source")
