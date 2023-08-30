from datetime import datetime

from typing import List, Any, Optional, Dict
from sqlalchemy import cast, TEXT, sql

from . import general
from .. import models, EmbeddingTensor, Embedding
from ..session import session
from .. import enums


def get(project_id: str, embedding_id: str) -> Embedding:
    return (
        session.query(Embedding)
        .filter(Embedding.project_id == project_id, Embedding.id == embedding_id)
        .first()
    )


def get_first_running_encoder(project_id: str) -> Embedding:
    return (
        session.query(models.Embedding)
        .filter(
            models.Embedding.project_id == project_id,
            models.Embedding.state != enums.EmbeddingState.FINISHED.value,
            models.Embedding.state != enums.EmbeddingState.FAILED.value,
        )
        .first()
    )


def get_embedding_record_ids(project_id: str) -> List[str]:
    ids: List[Any] = (
        session.query(EmbeddingTensor.record_id)
        .filter(
            EmbeddingTensor.embedding_id == Embedding.id,
            Embedding.project_id == project_id,
        )
        .order_by(EmbeddingTensor.record_id)
        .distinct()
    )
    return [str(val) for val, in ids]


def get_embedding_by_name(project_id: str, embedding_name: str) -> Embedding:
    return (
        session.query(Embedding)
        .filter(Embedding.project_id == project_id, Embedding.name == embedding_name)
        .first()
    )


def get_embedding_id_and_type(project_id: str, embedding_name: str) -> Any:
    return (
        session.query(Embedding.id, Embedding.type)
        .filter(Embedding.project_id == project_id, Embedding.name == embedding_name)
        .first()
    )


def get_filter_attribute_type_dict(
    project_id: str, embedding_id: str
) -> Dict[str, str]:
    query = f""" 
    SELECT 
        jsonb_object_agg(NAME,
            CASE data_type
                WHEN '{enums.DataTypes.CATEGORY.value}' THEN 'TEXT'
                ELSE data_type
            END )
    FROM attribute a
    WHERE a.name = ANY(
        SELECT unnest(filter_attributes)
        FROM embedding e
        WHERE e.id = '{embedding_id}')
    AND a.project_id = '{project_id}' """

    result = general.execute_first(query)
    return result[0]


def get_all_embeddings() -> List[Embedding]:
    return session.query(Embedding).all()


def get_all_embeddings_by_project_id(project_id: str) -> List[Embedding]:
    return session.query(Embedding).filter(Embedding.project_id == project_id).all()


def get_finished_embeddings(project_id: str) -> List[Embedding]:
    return (
        session.query(Embedding)
        .filter(
            Embedding.project_id == project_id,
            Embedding.state == enums.EmbeddingState.FINISHED.value,
        )
        .all()
    )


def get_finished_embeddings_by_started_at(project_id: str) -> List[Embedding]:
    return (
        session.query(Embedding)
        .filter(
            Embedding.project_id == project_id,
            Embedding.state == enums.EmbeddingState.FINISHED.value,
        )
        .order_by(Embedding.started_at.asc())
        .all()
    )


def get_finished_attribute_embeddings() -> List[Any]:
    return (
        session.query(cast(Embedding.project_id, TEXT), cast(Embedding.id, TEXT))
        .filter(
            Embedding.state == enums.EmbeddingState.FINISHED.value,
            Embedding.type == enums.EmbeddingType.ON_ATTRIBUTE.value,
        )
        .all()
    )


def get_waiting_embeddings(project_id: str) -> List[Embedding]:
    return (
        session.query(Embedding)
        .filter(
            Embedding.project_id == project_id,
            Embedding.state == enums.EmbeddingState.WAITING.value,
        )
        .all()
    )


def get_tensor_data_ordered_query(embedding_id: str) -> str:
    return f"""
    SELECT et.data
    FROM embedding_tensor et
    WHERE et.embedding_id = '{embedding_id}'
    ORDER BY et.record_id
    """


def get_tensors_by_project_id(project_id: str) -> List[Any]:
    query = f"""
        SELECT
            et.embedding_id,
            et.record_id,
            et.data,
            et.sub_key
        FROM embedding_tensor et
        INNER JOIN embedding e
            ON et.embedding_id = e.id
        WHERE e.state = 'FINISHED' AND e.project_id = '{project_id}'
        """
    return general.execute_all(query)


def get_tensors_by_embedding_id(embedding_id: str) -> List[Any]:
    return (
        session.query(
            cast(models.EmbeddingTensor.record_id, TEXT),
            models.EmbeddingTensor.data,
        )
        .filter(models.EmbeddingTensor.embedding_id == embedding_id)
        .all()
    )


def get_record_ids_by_embedding_id(embedding_id: str) -> List[str]:
    record_ids = (
        session.query(cast(models.EmbeddingTensor.record_id, TEXT))
        .filter(models.EmbeddingTensor.embedding_id == embedding_id)
        .all()
    )
    return [record_id for record_id, in record_ids]


def get_tensors_by_record_ids(embedding_id: str, record_ids: List[str]) -> List[Any]:
    return (
        session.query(
            cast(models.EmbeddingTensor.record_id, TEXT), models.EmbeddingTensor.data
        )
        .filter(
            models.EmbeddingTensor.embedding_id == embedding_id,
            models.EmbeddingTensor.record_id.in_(record_ids),
        )
        .all()
    )


def get_tensors_and_attributes_for_qdrant(
    project_id: str,
    embedding_id: str,
    attributes_to_include: Optional[Dict[str, str]] = None,
) -> List[Any]:
    payload_selector = "NULL"
    if attributes_to_include and len(attributes_to_include) > 0:
        payload_selector = ""
        for attr, data_type in attributes_to_include.items():
            if payload_selector != "":
                payload_selector += ","
            if data_type != enums.DataTypes.TEXT.value:
                payload_selector += f"'{attr}', (r.\"data\"->'{attr}')::{data_type}"
            else:
                payload_selector += f"'{attr}', r.\"data\"->>'{attr}'"
        payload_selector = f"json_build_object({payload_selector}) payload"
    id_column = None
    if has_sub_key(project_id, embedding_id):
        id_column = "et.id"
    else:
        id_column = "r.id"
    query = f"""
    SELECT 
        {id_column}::TEXT, 
        et."data", 
        {payload_selector}
    FROM embedding_tensor et
    INNER JOIN record r
        ON et.project_id = r.project_id AND et.record_id = r.id
    WHERE et.project_id = '{project_id}' AND et.embedding_id = '{embedding_id}'
    """

    return general.execute_all(query)


def get_match_record_ids_to_qdrant_ids(
    project_id: str, embedding_id: str, ids: List[str], limit: int
) -> List[Any]:
    # "normal" attributes are stored in qdrant with the record id, embedding lists with the tensor id
    if not has_sub_key(project_id, embedding_id):
        return ids
    query = f"""
    SELECT et.record_id::TEXT
    FROM   embedding_tensor et
    JOIN   UNNEST('{{{",".join(ids)}}}'::uuid[]) WITH ORDINALITY t(id, ord) USING (id)
    GROUP BY et.record_id
    ORDER BY MIN(ord)
    LIMIT {limit}
    """
    return [r[0] for r in general.execute_all(query)]


def get_qdrant_limit_factor(
    project_id: str, embedding_id: str, default: int = 1
) -> int:
    query = f"""
    SELECT CEIL(AVG(max_key))::INTEGER
    FROM (
        SELECT record_id, MAX(sub_key + 1) max_key
        FROM embedding_tensor et
        WHERE project_id = '{project_id}' AND embedding_id = '{embedding_id}'
        GROUP BY record_id )x """
    value = general.execute_first(query)
    if value is None or value[0] is None:
        return default
    return value[0]


def get_manually_labeled_tensors_by_embedding_id(
    project_id: str,
    embedding_id: str,
) -> List[Any]:
    query = f"""
    SELECT et.record_id::TEXT,et.data
    FROM embedding_tensor et
    INNER JOIN (
        SELECT record_id
        FROM record_label_association rla
        WHERE rla.project_id = '{project_id}' AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
        GROUP BY record_id ) rla
        ON et.record_id = rla.record_id
    WHERE et.embedding_id = '{embedding_id}'
    """
    return general.execute_all(query)


def has_sub_key(
    project_id: str,
    embedding_id: str,
) -> bool:
    query = f"""
    SELECT sub_key
    FROM embedding_tensor et
    WHERE et.project_id = '{project_id}' AND et.embedding_id = '{embedding_id}'
    LIMIT 1
    """
    value = general.execute_first(query)
    if value is None or value[0] is None:
        return False
    return True


def get_not_manually_labeled_tensors_by_embedding_id(
    project_id: str,
    embedding_id: str,
) -> List[Any]:
    query = f"""
    SELECT et.record_id::TEXT,et.data
    FROM embedding_tensor et
    LEFT JOIN (
        SELECT record_id
        FROM record_label_association rla
        WHERE rla.project_id = '{project_id}' AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
        GROUP BY record_id ) rla
        ON et.record_id = rla.record_id
    WHERE et.embedding_id = '{embedding_id}' AND rla.record_id IS NULL
    """
    return general.execute_all(query)


def get_tensor_count(embedding_id: str) -> EmbeddingTensor:
    return (
        session.query(models.EmbeddingTensor)
        .filter(models.EmbeddingTensor.embedding_id == embedding_id)
        .count()
    )


def get_tensor(
    embedding_id: str, record_id: Optional[str] = None, sub_key: Optional[int] = None
) -> EmbeddingTensor:
    query = session.query(models.EmbeddingTensor).filter(
        models.EmbeddingTensor.embedding_id == embedding_id,
    )
    if record_id:
        query = query.filter(models.EmbeddingTensor.record_id == record_id)
    if sub_key:
        query = query.filter(models.EmbeddingTensor.sub_key == sub_key)

    return query.first()


def create(
    project_id: str,
    attribute_id: str,
    name: str,
    created_by: str,
    state: str = None,
    custom: bool = None,
    type: str = None,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    model: Optional[str] = None,
    platform: Optional[str] = None,
    api_token: Optional[str] = None,
    filter_attributes: Optional[List[str]] = None,
    additional_data: Optional[Any] = None,
    with_commit: bool = False,
) -> Embedding:
    embedding: Embedding = Embedding(
        created_by=created_by,
        project_id=project_id,
        attribute_id=attribute_id,
        name=name,
        custom=False,
        type=type,
        state=enums.EmbeddingState.INITIALIZING.value,
        started_at=started_at,
        finished_at=finished_at,
        additional_data=additional_data,
    )
    if custom:
        embedding.custom = custom

    if state:
        embedding.state = state

    if started_at:
        embedding.started_at = started_at

    if finished_at is not None:
        embedding.finished_at = finished_at

    if api_token:
        embedding.api_token = api_token

    if model:
        embedding.model = model

    if platform:
        embedding.platform = platform

    if filter_attributes:
        embedding.filter_attributes = filter_attributes

    if additional_data:
        embedding.additional_data = additional_data
    else:
        embedding.additional_data = {}

    general.add(embedding, with_commit)
    return embedding


def create_tensor(
    project_id: str,
    record_id: str,
    embedding_id: str,
    data: List[float],
    sub_key: Optional[int] = None,
    with_commit: bool = False,
) -> EmbeddingTensor:
    embedding_tensor: EmbeddingTensor = EmbeddingTensor(
        project_id=project_id,
        record_id=record_id,
        embedding_id=embedding_id,
        data=data,
        sub_key=sub_key,
    )
    general.add(embedding_tensor, with_commit)
    return embedding_tensor


def create_tensors(
    project_id: str,
    embedding_id: str,
    record_ids: List[str],
    tensors: List[List[float]],
    with_commit: bool = False,
) -> None:
    to_add = None
    if len(record_ids) > 0 and "@" in record_ids[0]:

        to_add = [
            EmbeddingTensor(
                project_id=project_id,
                record_id=record_id.split("@")[0],
                embedding_id=embedding_id,
                data=tensor,
                sub_key=int(record_id.split("@")[1]),
            )
            for record_id, tensor in zip(record_ids, tensors)
        ]
    else:
        to_add = [
            EmbeddingTensor(
                project_id=project_id,
                record_id=record_id,
                embedding_id=embedding_id,
                data=tensor,
            )
            for record_id, tensor in zip(record_ids, tensors)
        ]
    general.add_all(to_add, with_commit)


def update_similarity_threshold(
    project_id: str,
    embedding_id: str,
    similarity_threshold: float,
    with_commit: bool = False,
) -> None:
    embedding_item = get(project_id, embedding_id)
    embedding_item.similarity_threshold = similarity_threshold
    general.flush_or_commit(with_commit)


def update_embedding_state_encoding(
    project_id: str, embedding_id: str, with_commit: bool = False
) -> None:
    __update_embedding_state(
        project_id, embedding_id, enums.EmbeddingState.ENCODING.value, with_commit
    )


def update_embedding_state_finished(
    project_id: str, embedding_id: str, with_commit: bool = False
) -> None:
    embedding_item = get(project_id, embedding_id)
    embedding_item.finished_at = sql.func.now()
    __update_embedding_state(
        project_id, embedding_id, enums.EmbeddingState.FINISHED.value, with_commit
    )


def update_embedding_state_failed(
    project_id: str, embedding_id: str, with_commit: bool = False
) -> None:
    __update_embedding_state(
        project_id, embedding_id, enums.EmbeddingState.FAILED.value, with_commit
    )


def update_embedding_state_waiting(
    project_id: str, embedding_id: str, with_commit: bool = False
) -> None:
    __update_embedding_state(
        project_id, embedding_id, enums.EmbeddingState.WAITING.value, with_commit
    )


def __update_embedding_state(
    project_id: str, embedding_id: str, state: str, with_commit=False
) -> None:
    embedding_item = get(project_id, embedding_id)
    embedding_item.state = state
    general.flush_or_commit(with_commit)


def delete(project_id: str, embedding_id: str, with_commit: bool = False) -> None:
    session.query(Embedding).filter(
        Embedding.project_id == project_id, Embedding.id == embedding_id
    ).delete()
    general.flush_or_commit(with_commit)


def delete_tensors(embedding_id: str, with_commit: bool = False) -> None:
    session.query(EmbeddingTensor).filter(EmbeddingTensor.id == embedding_id).delete()
    general.flush_or_commit(with_commit)


def update_embedding_filter_attributes(
    project_id: str,
    embedding_id: str,
    filter_attributes: List[str],
    with_commit: bool = False,
) -> None:
    embedding_item = get(project_id, embedding_id)
    embedding_item.filter_attributes = filter_attributes
    general.flush_or_commit(with_commit)
