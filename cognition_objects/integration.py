from typing import List, Optional, Dict
from datetime import datetime
from fastapi import HTTPException

from ..business_objects import general
from ..session import session
from ..models import CognitionIntegration
from ..enums import (
    CognitionMarkdownFileState,
    CognitionIntegrationType,
)


def get_by_id(id: str) -> CognitionIntegration:
    return (
        session.query(CognitionIntegration)
        .filter(CognitionIntegration.id == id)
        .first()
    )


def get(project_id: str, name: str) -> CognitionIntegration:
    return (
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.project_id == project_id,
            CognitionIntegration.name == name,
        )
        .first()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionIntegration]:
    return (
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.project_id == project_id,
        )
        .order_by(CognitionIntegration.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    state: str,
    integration_type: CognitionIntegrationType,
    integration_config: Dict,
    llm_config: Dict,
    extract_history: Optional[Dict] = {},
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
) -> CognitionIntegration:
    if state not in CognitionMarkdownFileState.all():
        raise HTTPException(status_code=400, detail=f"Invalid state: {state}")
    integration: CognitionIntegration = CognitionIntegration(
        id=id,
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        state=state,
        type=integration_type.value,
        config=integration_config,
        llm_config=llm_config,
        extract_history=extract_history,
    )
    general.add(integration, with_commit)

    return integration


def update(
    id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    state: Optional[CognitionMarkdownFileState] = None,
    integration_config: Optional[int] = None,
    llm_config: Optional[Dict] = None,
    error_message: Optional[str] = None,
    extract_history: Optional[Dict] = None,
    with_commit: bool = True,
) -> CognitionIntegration:
    integration: CognitionIntegration = get_by_id(id)

    if name is not None:
        integration.name = name
    if description is not None:
        integration.description = description
    if state is not None:
        if state not in CognitionMarkdownFileState.all():
            raise HTTPException(status_code=400, detail=f"Invalid state: {state}")
        integration.state = state
    if integration_config is not None:
        integration.config = integration_config
    if llm_config is not None:
        integration.llm_config = llm_config
    if error_message is not None:
        integration.error_message = error_message
    if extract_history is not None:
        integration.extract_history = extract_history

    general.add(integration, with_commit)
    return integration


def execution_finished(id: str) -> bool:
    return bool(
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.id == id,
            CognitionIntegration.state == CognitionMarkdownFileState.FINISHED.value,
        )
        .first()
    )


def clear_history(id: str) -> None:
    integration: CognitionIntegration = get_by_id(id)
    integration.extract_history = {}
    integration.state = CognitionMarkdownFileState.QUEUE.value
    general.add(integration, True)


def delete(id: str, with_commit: bool = True) -> None:
    session.query(CognitionIntegration).filter(CognitionIntegration.id == id).delete()
    general.flush_or_commit(with_commit)
