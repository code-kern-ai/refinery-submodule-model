from typing import List, Optional, Dict
from datetime import datetime
from fastapi import HTTPException

from ..business_objects import general
from ..session import session
from ..models import CognitionThirdPartyIntegration
from ..enums import CognitionMarkdownFileState


def get_by_id(id: str) -> CognitionThirdPartyIntegration:
    return (
        session.query(CognitionThirdPartyIntegration)
        .filter(CognitionThirdPartyIntegration.id == id)
        .first()
    )


def get(project_id: str, name: str) -> CognitionThirdPartyIntegration:
    return (
        session.query(CognitionThirdPartyIntegration)
        .filter(
            CognitionThirdPartyIntegration.project_id == project_id,
            CognitionThirdPartyIntegration.name == name,
        )
        .first()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionThirdPartyIntegration]:
    return (
        session.query(CognitionThirdPartyIntegration)
        .filter(
            CognitionThirdPartyIntegration.project_id == project_id,
        )
        .order_by(CognitionThirdPartyIntegration.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    state: str,
    integration_type: str,
    integration_config: Dict,
    llm_config: Dict,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
) -> CognitionThirdPartyIntegration:
    if state not in CognitionMarkdownFileState.all():
        raise HTTPException(status_code=400, detail=f"Invalid state: {state}")
    integration: CognitionThirdPartyIntegration = CognitionThirdPartyIntegration(
        id=id,
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        state=state,
        type=integration_type,
        config=integration_config,
        llm_config=llm_config,
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
    with_commit: bool = True,
) -> CognitionThirdPartyIntegration:
    integration: CognitionThirdPartyIntegration = get_by_id(id)

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
    general.flush_or_commit(with_commit)
    return integration


def execution_finished(id: str) -> bool:
    return bool(
        session.query(CognitionThirdPartyIntegration)
        .filter(
            CognitionThirdPartyIntegration.id == id,
            CognitionThirdPartyIntegration.state
            == CognitionMarkdownFileState.FINISHED.value,
        )
        .first()
    )


def delete(id: str, with_commit: bool = True) -> None:
    session.query(CognitionThirdPartyIntegration).filter(
        CognitionThirdPartyIntegration.id == id
    ).delete()
    general.flush_or_commit(with_commit)
