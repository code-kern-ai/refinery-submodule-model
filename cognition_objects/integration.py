from typing import List, Optional, Dict
import datetime
from fastapi import HTTPException
from sqlalchemy import func

from ..business_objects import general
from ..session import session
from ..models import CognitionIntegration, Project
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


def get_all(integration_type: Optional[str] = None) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration)
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    return query.order_by(CognitionIntegration.created_at).all()


def get_all_in_org(
    org_id: str, integration_type: Optional[str] = None
) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration).filter(
        CognitionIntegration.organization_id == org_id
    )
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    return query.order_by(CognitionIntegration.created_at).all()


def get_all_in_org_paginated(
    org_id: str,
    integration_type: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
) -> List[CognitionIntegration]:
    schema_name = CognitionIntegration.__table__.schema or "public"
    table_name = f"{schema_name}.{CognitionIntegration.__tablename__}"

    first_page = (page - 1) * page_size
    last_page = page * page_size

    sql = f"""
    SELECT id FROM (
        SELECT 
            ROW_NUMBER () OVER(PARTITION BY intg.id ORDER BY intg.created_at ASC) rn,
            intg.id
        FROM {table_name} intg
        WHERE intg.organization_id = '{org_id}'
    ) pages
    WHERE rn BETWEEN {first_page} AND {last_page}
    """
    integration_ids = general.execute_all(sql)
    if not integration_ids:
        return []

    query = session.query(CognitionIntegration).filter(
        CognitionIntegration.id.in_([row[0] for row in integration_ids])
    )
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    return query.order_by(CognitionIntegration.created_at.desc()).all()


def get_all_by_project_id(project_id: str) -> List[CognitionIntegration]:
    return (
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.project_id == project_id,
        )
        .order_by(CognitionIntegration.created_at.asc())
        .all()
    )


def count_org_integrations(org_id: str) -> int:
    counts = (
        session.query(CognitionIntegration.type, func.count(CognitionIntegration.id))
        .filter(
            CognitionIntegration.organization_id == org_id,
        )
        .group_by(CognitionIntegration.type)
        .all()
    )
    return {cognition_type: count for cognition_type, count in counts}


def create(
    org_id: str,
    user_id: str,
    name: str,
    description: str,
    tokenizer: str,
    state: str,
    integration_type: CognitionIntegrationType,
    integration_config: Dict,
    llm_config: Dict,
    started_at: Optional[datetime.datetime] = None,
    created_at: Optional[datetime.datetime] = None,
    finished_at: Optional[datetime.datetime] = None,
    id: Optional[str] = None,
    project_id: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionIntegration:
    if state not in CognitionMarkdownFileState.all():
        raise HTTPException(status_code=400, detail=f"Invalid state: {state}")
    integration: CognitionIntegration = CognitionIntegration(
        id=id,
        organization_id=org_id,
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        started_at=started_at,
        finished_at=finished_at,
        name=name,
        description=description,
        tokenizer=tokenizer,
        state=state,
        type=integration_type.value,
        config=integration_config,
        llm_config=llm_config,
    )
    general.add(integration, with_commit)

    return integration


def update(
    id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tokenizer: Optional[str] = None,
    state: Optional[CognitionMarkdownFileState] = None,
    integration_config: Optional[int] = None,
    llm_config: Optional[Dict] = None,
    error_message: Optional[str] = None,
    started_at: Optional[datetime.datetime] = None,
    finished_at: Optional[datetime.datetime] = None,
    last_synced_at: Optional[datetime.datetime] = None,
    is_synced: Optional[bool] = None,
    with_commit: bool = True,
) -> CognitionIntegration:
    integration: CognitionIntegration = get_by_id(id)

    if name is not None:
        integration.name = name
    if description is not None:
        integration.description = description
    if tokenizer is not None:
        integration.tokenizer = tokenizer
    if state is not None:
        integration.state = state.value
    if integration_config is not None:
        integration.config = integration_config
    if llm_config is not None:
        integration.llm_config = llm_config
    if error_message is not None:
        integration.error_message = error_message
    if started_at is not None:
        integration.started_at = started_at
    if last_synced_at is not None:
        integration.last_synced_at = last_synced_at

    integration.is_synced = is_synced
    integration.finished_at = finished_at

    general.add(integration, with_commit)
    return integration


def execution_finished(id: str) -> bool:
    return bool(
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.id == id,
            CognitionIntegration.state.in_(
                [
                    CognitionMarkdownFileState.FINISHED.value,
                    CognitionMarkdownFileState.FAILED.value,
                ]
            ),
        )
        .first()
    )


def clear_history(id: str) -> None:
    integration: CognitionIntegration = get_by_id(id)
    integration.extract_history = {}
    integration.state = CognitionMarkdownFileState.QUEUE.value
    general.add(integration, True)


def delete_many(
    ids: List[str], delete_refinery_projects: bool = False, with_commit: bool = True
) -> None:
    integrations = session.query(CognitionIntegration).filter(
        CognitionIntegration.id.in_(ids)
    )
    if delete_refinery_projects:
        session.query(Project).filter(
            Project.id.in_(filter(None, [i.project_id for i in integrations]))
        ).delete(synchronize_session=False)
    integrations.delete(synchronize_session=False)
    general.flush_or_commit(with_commit)
