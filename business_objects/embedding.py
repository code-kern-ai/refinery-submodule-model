from typing import List, Any, Optional
from sqlalchemy import cast, TEXT

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


def get_embedding_id_and_type(project_id: str, embedding_name: str) -> Any:
    return (
        session.query(Embedding.id, Embedding.type)
        .filter(Embedding.project_id == project_id, Embedding.name == embedding_name)
        .first()
    )


def get_finished_embeddings(project_id: str) -> List[Embedding]:
    return (
        session.query(Embedding)
        .filter(
            Embedding.project_id == project_id,
            Embedding.state == enums.EmbeddingState.FINISHED.value,
        )
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
            et.data
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


def get_tensor(embedding_id: str, record_id: Optional[str] = None) -> EmbeddingTensor:

    query = session.query(models.EmbeddingTensor).filter(
        models.EmbeddingTensor.embedding_id == embedding_id,
    )
    if record_id:
        query = query.filter(models.EmbeddingTensor.record_id == record_id)

    return query.first()


def create(
    project_id: str,
    attribute_id: str,
    name: str,
    state: str = None,
    custom: bool = None,
    type: str = None,
    with_commit: bool = False,
) -> Embedding:
    embedding: Embedding = Embedding(
        project_id=project_id,
        attribute_id=attribute_id,
        name=name,
        custom=False,
        type=type,
        state=enums.EmbeddingState.INITIALIZING.value,
    )
    if custom:
        embedding.custom = custom

    if state:
        embedding.state = state

    general.add(embedding, with_commit)
    return embedding


def create_tensor(
    project_id: str,
    record_id: str,
    embedding_id: str,
    data: List[float],
    with_commit: bool = False,
) -> EmbeddingTensor:
    embedding_tensor: EmbeddingTensor = EmbeddingTensor(
        project_id=project_id,
        record_id=record_id,
        embedding_id=embedding_id,
        data=data,
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
    tensors = [
        EmbeddingTensor(
            project_id=project_id,
            record_id=record_id,
            embedding_id=embedding_id,
            data=tensor,
        )
        for record_id, tensor in zip(record_ids, tensors)
    ]
    general.add_all(tensors, with_commit)


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
