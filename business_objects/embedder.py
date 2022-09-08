from datetime import datetime
from typing import Dict, List, Any, Optional

from . import general
from ..models import (
    Embedder,
    EmbedderPayload,
)
from ..session import session


def get(project_id: str, source_id: str) -> Embedder:
    return (
        session.query(Embedder)
        .filter(
            Embedder.project_id == project_id,
            Embedder.id == source_id,
        )
        .first()
    )


def get_all(project_id: str) -> List[Embedder]:
    return (
        session.query(Embedder)
        .filter(
            Embedder.project_id == project_id,
        )
        .all()
    )


def get_payload(project_id: str, payload_id: str) -> EmbedderPayload:
    return (
        session.query(EmbedderPayload)
        .filter(
            EmbedderPayload.project_id == project_id,
            EmbedderPayload.id == payload_id,
        )
        .first()
    )


def get_last_payload(project_id: str, source_id: str) -> EmbedderPayload:
    return (
        session.query(EmbedderPayload)
        .filter(
            EmbedderPayload.project_id == project_id,
            EmbedderPayload.embedder_id == source_id,
        )
        .order_by(EmbedderPayload.created_at.desc())
        .first()
    )


def get_payloads_by_project_id(project_id: str) -> List[Any]:
    query: str = f"""
        SELECT 
            payload.id,
            payload.embedder_id,
            payload.created_at,
            payload.finished_at,
            payload.iteration,
            payload.source_code,
            payload.logs,
            payload.state
        FROM 
            embedder_payload AS payload
        INNER JOIN
            embedder 
        ON 
            payload.embedder_id=embedder.id
        WHERE 
            embedder.project_id='{project_id}'
        ;
        """
    return general.execute_all(query)


def get_overview_data(project_id: str) -> List[Dict[str, Any]]:
    query = f"""
    SELECT array_agg(row_to_json(data_select))
    FROM (
        SELECT 
            _is.id,
            _is.name,
            _is.type "embedderType",
            _is.description,
            _is.created_at "createdAt",
            _is.created_by "createdBy",
            isp.state,
            isp.created_at "lastRun",
        FROM embedder _is
        LEFT JOIN LATERAL(
            SELECT isp.id,isp.state,isp.created_at
            FROM embedder_payload isp
            WHERE _is.id = isp.embedder_id 
            AND _is.project_id = isp.project_id
            ORDER BY isp.iteration DESC
            LIMIT 1
        ) isp ON TRUE
        WHERE _is.project_id = '{project_id}'
        ORDER BY "createdAt" DESC,name
        )data_select """
    values = general.execute_first(query)

    if values:
        return values[0]


def continue_payload(project_id: str, embedder_id: str, payload_id: str) -> bool:
    query = f"""
    SELECT isp.state
    FROM embedder_payload isp
    INNER JOIN embedder _is
        ON isp.embedder_id = _is.id AND isp.project_id = _is.project_id
    WHERE isp.id = '{payload_id}' 
    AND isp.source_id = '{embedder_id}' 
    AND isp.project_id = '{project_id}' """

    value = general.execute_first(query)
    if not value or value[0] != "CREATED":
        return False
    return True


def create(
    project_id: str,
    name: str,
    type: str,
    description: str,
    source_code: str,
    version: Optional[int] = None,
    created_at: Optional[datetime] = None,
    created_by: Optional[str] = None,
    with_commit: bool = False,
) -> Embedder:
    embedder: Embedder = Embedder(
        project_id=project_id,
        name=name,
        type=type,
        description=description,
        source_code=source_code,
        version=version,
        created_at=created_at,
        created_by=created_by,
    )
    general.add(embedder, with_commit)
    return embedder


def create_payload(
    project_id: str,
    embedder_id: str,
    state: str,
    created_by: Optional[str] = None,
    created_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    iteration: Optional[int] = None,
    source_code: Optional[str] = None,
    logs: List[str] = None,
    with_commit: bool = False,
) -> EmbedderPayload:
    payload: EmbedderPayload = EmbedderPayload(
        embedder_id=embedder_id, project_id=project_id, state=state
    )
    if created_by:
        payload.created_by = created_by
    if iteration:
        payload.iteration = iteration
    if source_code:
        payload.source_code = source_code
    if created_at:
        payload.created_at = created_at
    if finished_at:
        payload.finished_at = finished_at
    if logs:
        payload.logs = logs
    general.add(payload, with_commit)
    return payload


def delete(project_id: str, embedder_id: str, with_commit: bool = False) -> None:
    session.query(Embedder).filter(
        Embedder.project_id == project_id,
        Embedder.id == embedder_id,
    ).delete()
    general.flush_or_commit(with_commit)


def update(
    project_id: str,
    embedder_id: str,
    name: Optional[str] = None,
    type: Optional[str] = None,
    description: Optional[str] = None,
    source_code: Optional[str] = None,
    version: Optional[int] = None,
    created_at: Optional[datetime] = None,
    created_by: Optional[str] = None,
    with_commit: bool = False,
) -> None:
    information_source = get(project_id, embedder_id)

    if name is not None:
        information_source.name = name
    if type is not None:
        information_source.type = type
    if description is not None:
        information_source.description = description
    if source_code is not None:
        information_source.source_code = source_code
    if version is not None:
        information_source.version = version
    if created_at is not None:
        information_source.created_at = created_at
    if created_by is not None:
        information_source.created_at = created_by
    general.flush_or_commit(with_commit)


def update_payload(
    project_id: str,
    payload_id: str,
    state: str = None,
    progress: float = None,
    with_commit: bool = False,
) -> None:
    payload_item = get_payload(project_id, payload_id)
    if not payload_item:
        return
    if state:
        payload_item.state = state
    if progress:
        payload_item.progress = progress
    general.flush_or_commit(with_commit)
