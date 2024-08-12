from datetime import datetime

from typing import List, Any, Optional, Dict, Iterable, Tuple
from sqlalchemy import cast, TEXT, sql

from . import general
from .. import models, EmbeddingTensor, Embedding
from ..session import session
from .. import enums

from ..util import prevent_sql_injection


def get(project_id: str, embedding_id: str) -> Embedding:
    return (
        session.query(Embedding)
        .filter(Embedding.project_id == project_id, Embedding.id == embedding_id)
        .first()
    )


def get_all_by_attribute_ids(
    project_id: str, attribute_ids: Iterable[str]
) -> List[Embedding]:
    return (
        session.query(Embedding)
        .filter(
            Embedding.project_id == project_id,
            Embedding.attribute_id.in_(attribute_ids),
        )
        .all()
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
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
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


def get_finished_embeddings_dropdown_list(project_id: str) -> List[Dict[str, str]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT array_agg(jsonb_build_object('value', e.id,'name',e.NAME))
    FROM embedding e
    WHERE e.project_id = '{project_id}' AND e.state = '{enums.EmbeddingState.FINISHED.value}' AND e.type = '{enums.EmbeddingType.ON_ATTRIBUTE.value}' """
    values = general.execute_first(query)

    if values and values[0]:
        return values[0]
    return []


def get_tensor_data_ordered_query(embedding_id: str) -> str:
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
    return f"""
    SELECT et.data
    FROM embedding_tensor et
    WHERE et.embedding_id = '{embedding_id}'
    ORDER BY et.record_id
    """


def get_tensors_by_project_id(project_id: str) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
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


def get_tensor_ids_and_record_ids_by_embedding_id(
    embedding_id: str, record_ids: List[str] = None
) -> List[Any]:
    query = session.query(
        cast(models.EmbeddingTensor.id, TEXT),
        cast(models.EmbeddingTensor.record_id, TEXT),
    ).filter(models.EmbeddingTensor.embedding_id == embedding_id)
    if record_ids:
        query = query.filter(models.EmbeddingTensor.record_id.in_(record_ids))
    return query.all()


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


def __build_payload_selector(
    attributes_to_include: Optional[Dict[str, str]] = None,
) -> str:
    # empty json object to extend by label data later
    payload_selector = "jsonb_build_object()"

    if attributes_to_include and len(attributes_to_include) > 0:
        payload_selector = ""
        for attr, data_type in attributes_to_include.items():
            if payload_selector != "":
                payload_selector += ","
            if data_type != enums.DataTypes.TEXT.value:
                payload_selector += f"'{attr}', (r.\"data\"->>'{attr}')::{data_type}"
            else:
                payload_selector += f"'{attr}', r.\"data\"->>'{attr}'"
        payload_selector = f"json_build_object({payload_selector}) payload"
    return payload_selector


def get_attributes_for_qdrant(
    project_id: str,
    record_ids: Optional[List[str]] = None,
    attributes_to_include: Optional[Dict[str, str]] = None,
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    if record_ids:
        record_ids = [
            prevent_sql_injection(id, isinstance(id, str)) for id in record_ids
        ]
    if attributes_to_include:
        attributes_to_include = {
            prevent_sql_injection(key, isinstance(key, str)): prevent_sql_injection(
                value, isinstance(value, str)
            )
            for key, value in attributes_to_include.items()
        }
    payload_selector = __build_payload_selector(attributes_to_include)
    query = f"""
    SELECT
        r.id::TEXT record_id,
        {payload_selector}
    FROM record r
    WHERE r.project_id = '{project_id}'
    """
    if record_ids:
        query += f" AND r.id IN ('{','.join(record_ids)}')"
    return general.execute_all(query)


def get_tensors_and_attributes_for_qdrant(
    project_id: str,
    embedding_id: str,
    attributes_to_include: Optional[Dict[str, str]] = None,
    record_ids: Optional[List[str]] = None,
    only_tensor_ids: bool = False,
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
    if record_ids:
        record_ids = [
            prevent_sql_injection(id, isinstance(id, str)) for id in record_ids
        ]
    if attributes_to_include:
        attributes_to_include = {
            prevent_sql_injection(key, isinstance(key, str)): prevent_sql_injection(
                value, isinstance(value, str)
            )
            for key, value in attributes_to_include.items()
        }
    payload_selector = __build_payload_selector(attributes_to_include)
    query = f"""
    SELECT
        r.id::TEXT record_id,
        {'et."data", ' if not only_tensor_ids else ''}{payload_selector},
        et.id::TEXT tensor_id
    FROM embedding_tensor et
    INNER JOIN record r
        ON et.project_id = r.project_id AND et.record_id = r.id
    WHERE et.project_id = '{project_id}' AND et.embedding_id = '{embedding_id}'
    """
    if record_ids:
        query += f" AND r.id IN ('{','.join(record_ids)}')"

    return general.execute_all(query)


def get_match_record_ids_to_qdrant_ids(
    project_id: str, embedding_id: str, ids: List[str], limit: int
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
    ids = [prevent_sql_injection(id, isinstance(id, str)) for id in ids]
    limit = prevent_sql_injection(limit, isinstance(limit, int))
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


def get_match_record_ids_to_qdrant_ids_with_max_score(
    project_id: str, embedding_id: str, qdrant_results: List[Any], limit: int
) -> List[Any]:
    ## Currently an open question how to handle multiple entries per record
    ## We are using the min score (distance) though this probably isn't optimal for a final solution
    ## e.g. how to interpret a record with 3 semi relevant entries all with distance ~40 vs 1 with 35, 1 with 10 or 1 with 0.0001
    ## with min it's 3x 40 is less relevant than e.g. 1x 0.0001. Imo very reasonable to prioritize a very likely relevant over the 3 maybe relevant.
    ## For higher scores however maybe not? as a record with 3x 40 is potentially more relevant than 1x 35

    if len(qdrant_results) == 0:
        return []

    # "normal" attributes are stored in qdrant with the record id, embedding lists with the tensor id, no need to query
    if not has_sub_key(project_id, embedding_id):
        return [{"id": entry.id, "score": entry.score} for entry in qdrant_results]

    CHUNK_SIZE = 100
    if len(qdrant_results) > CHUNK_SIZE:
        results = []
        for i in range(0, len(qdrant_results), CHUNK_SIZE):
            chunk = qdrant_results[i : i + CHUNK_SIZE]
            if r := get_match_record_ids_to_qdrant_ids_with_max_score(
                project_id, embedding_id, chunk, limit
            ):
                results.extend(r)
        return results

    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))

    if len(qdrant_results) == 0:
        return []

    query = f"""
    {__generate_with_table_union_query(qdrant_results)}

    SELECT et.record_id::TEXT id, MAX(s.score) score
    FROM  embedding_tensor et
    INNER JOIN scores s
        ON et.id = s.id
    WHERE et.project_id = '{project_id}' AND et.embedding_id = '{embedding_id}'
    GROUP BY et.record_id
    ORDER BY 2 ASC
    LIMIT {limit}
    """

    return [{"id": r[0], "score": r[1]} for r in general.execute_all(query)]


def __generate_with_table_union_query(qdrant_results: List[Any]) -> str:

    if len(qdrant_results) == 0:
        union_query = None
    else:
        union_query = "UNION ALL " + " UNION ALL".join(
            [f"\nSELECT '{entry.id}', {entry.score}" for entry in qdrant_results]
        )

    return f"""
    WITH scores AS (
        SELECT id::UUID, score::FLOAT
        FROM (
            SELECT NULL id, NULL score
            {union_query}
        ) x
    ) """


def get_qdrant_limit_factor(
    project_id: str, embedding_id: str, default: int = 1
) -> int:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
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
    limit: Optional[int] = None,
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
    add_limit = ""
    if limit:
        limit = prevent_sql_injection(limit, isinstance(limit, int))
        add_limit = f"""
        ORDER BY random()
        LIMIT {limit}
        """
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
    {add_limit}
    """
    return general.execute_all(query)


def has_sub_key(
    project_id: str,
    embedding_id: str,
) -> bool:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
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
    limit: Optional[int] = None,
) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
    if limit:
        limit = prevent_sql_injection(limit, isinstance(limit, int))
        add_limit = f"""
        ORDER BY random()
        LIMIT {limit}
        """
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
    {add_limit}
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
    # added @ as sub_key (list index) for embedding list attributes -> record_id@sub_key
    # basically the reversal of record.get_attribute_data for embedding lists
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
    if embedding_item:
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


def delete_by_record_ids(
    project_id: str,
    embedding_id: str,
    record_ids: Iterable[str],
    with_commit: bool = False,
) -> None:
    session.query(EmbeddingTensor).filter(
        EmbeddingTensor.project_id == project_id,
        EmbeddingTensor.embedding_id == embedding_id,
        EmbeddingTensor.record_id.in_(record_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def delete_by_record_ids_and_sub_keys(
    project_id: str,
    embedding_id: str,
    to_del: Iterable[Tuple[str, str]],
    with_commit: bool = False,
) -> None:
    # deletes entries based on record_id and sub_key tuples for record changes
    if len(to_del) == 0:
        return
    if len(to_del) > 100:
        # since chunk_list isn't available here, we raise an error
        raise ValueError("Too many tuples to delete at once. Please chunk beforehand.")

    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    embedding_id = prevent_sql_injection(embedding_id, isinstance(embedding_id, str))
    query_adds = [
        (prevent_sql_injection(r), prevent_sql_injection(s)) for r, s in to_del
    ]

    query_adds = [
        f"(et.record_id = '{record_id}' AND et.sub_key = {sub_key})"
        for record_id, sub_key in to_del
    ]

    query_add = " OR ".join(query_adds)

    query = f"""
    DELETE FROM embedding_tensor
    WHERE project_id = '{project_id}' AND id IN ( 
    SELECT et.id
    FROM embedding_tensor et
    WHERE et.project_id = '{project_id}' AND et.embedding_id = '{embedding_id}'
    AND ({query_add}) ) """

    general.execute(query)
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
