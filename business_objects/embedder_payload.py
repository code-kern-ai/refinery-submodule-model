from datetime import datetime
from typing import List, Optional

from . import general
from .. import enums
from ..models import Embedder, EmbedderPayload
from ..session import session


def get(project_id: str, payload_id: str) -> EmbedderPayload:
    return (
        session.query(EmbedderPayload)
        .filter(
            Embedder.project_id == project_id,
            Embedder.id == EmbedderPayload.embedder_id,
            EmbedderPayload.id == payload_id,
        )
        .first()
    )


def create(
    project_id: str,
    source_code: str,
    state: enums.PayloadState,
    iteration: int,
    embedder_id: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> EmbedderPayload:
    payload: EmbedderPayload = EmbedderPayload(
        source_code=source_code,
        state=state.value,
        iteration=iteration,
        embedder_id=embedder_id,
        created_by=created_by,
        project_id=project_id,
        created_at=created_at,
    )
    general.add(payload, with_commit)
    return payload
