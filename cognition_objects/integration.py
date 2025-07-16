from typing import List, Optional, Dict, Union, Any
import datetime
from sqlalchemy import func
from sqlalchemy.orm.attributes import flag_modified

from ..business_objects import general
from ..session import session
from ..models import CognitionIntegration, CognitionGroup
from ..enums import (
    CognitionMarkdownFileState,
    CognitionIntegrationType,
)
from ..util import prevent_sql_injection

FINISHED_STATES = [
    CognitionMarkdownFileState.FINISHED.value,
    CognitionMarkdownFileState.FAILED.value,
]


def get_by_ids(ids: List[str]) -> List[CognitionIntegration]:
    return (
        session.query(CognitionIntegration)
        .filter(CognitionIntegration.id.in_(ids))
        .all()
    )


def get_by_id(id: str) -> CognitionIntegration:
    return (
        session.query(CognitionIntegration)
        .filter(CognitionIntegration.id == id)
        .first()
    )


def get_all(
    integration_type: Optional[str] = None,
    exclude_failed: bool = False,
    only_synced: bool = False,
) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration)
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    if exclude_failed:
        query = query.filter(
            CognitionIntegration.state != CognitionMarkdownFileState.FAILED.value
        )
    if only_synced:
        query = query.filter(CognitionIntegration.is_synced == True)
    return query.order_by(CognitionIntegration.created_at.desc()).all()


def get_all_in_org(
    org_id: str,
    integration_type: Optional[str] = None,
    only_synced: bool = False,
    exclude_failed: bool = False,
) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration).filter(
        CognitionIntegration.organization_id == org_id
    )
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    if only_synced:
        query = query.filter(CognitionIntegration.is_synced == True)
    if exclude_failed:
        query = query.filter(
            CognitionIntegration.state != CognitionMarkdownFileState.FAILED.value
        )
    return query.order_by(CognitionIntegration.created_at.desc()).all()


def get_all_in_org_paginated(
    org_id: str,
    integration_type: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
) -> List[CognitionIntegration]:
    query = session.query(CognitionIntegration).filter(
        CognitionIntegration.organization_id == org_id,
    )

    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)

    return (
        query.order_by(CognitionIntegration.created_at.desc())
        .limit(page_size)
        .offset(max(0, (page - 1) * page_size))
        .all()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionIntegration]:
    return (
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.project_id == project_id,
        )
        .order_by(CognitionIntegration.created_at.desc())
        .all()
    )


def get_last_synced_at(
    org_id: str, integration_type: Optional[str] = None
) -> datetime.datetime:
    query = session.query(func.max(CognitionIntegration.last_synced_at)).filter(
        CognitionIntegration.organization_id == org_id
    )
    if integration_type:
        query = query.filter(CognitionIntegration.type == integration_type)
    result = query.first()
    return result[0] if result else None


def count_org_integrations(org_id: str) -> Dict[str, int]:
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
    integration: CognitionIntegration = CognitionIntegration(
        id=id,
        organization_id=org_id,
        project_id=project_id,
        created_by=user_id,
        updated_by=user_id,
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
        delta_criteria={"delta_url": None},
    )
    general.add(integration, with_commit)

    return integration


def update(
    id: str,
    updated_by: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tokenizer: Optional[str] = None,
    state: Optional[CognitionMarkdownFileState] = None,
    integration_config: Optional[int] = None,
    llm_config: Optional[Dict] = None,
    error_message: Optional[str] = None,
    started_at: Optional[datetime.datetime] = None,
    finished_at: Optional[Union[str, datetime.datetime]] = None,
    last_synced_at: Optional[datetime.datetime] = None,
    is_synced: Optional[Union[str, bool]] = None,
    delta_criteria: Optional[Dict[str, str]] = None,
    with_commit: bool = True,
) -> Optional[CognitionIntegration]:
    integration: CognitionIntegration = get_by_id(id)
    if not integration:
        return None

    if updated_by is not None:
        integration.updated_by = updated_by
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
        flag_modified(integration, "config")
    if llm_config is not None:
        integration.llm_config = llm_config
        flag_modified(integration, "llm_config")
    if started_at is not None:
        integration.started_at = started_at
    if last_synced_at is not None:
        integration.last_synced_at = last_synced_at
    if delta_criteria is not None:
        integration.delta_criteria = delta_criteria
        flag_modified(integration, "delta_criteria")
    if error_message is not None:
        if error_message == "NULL":
            integration.error_message = None
        else:
            integration.error_message = error_message
    if is_synced is not None:
        if is_synced == "NULL":
            integration.is_synced = None
        else:
            integration.is_synced = is_synced
    if finished_at is not None:
        if finished_at == "NULL":
            integration.finished_at = None
        else:
            integration.finished_at = finished_at

    general.add(integration, with_commit)
    return integration


def execution_finished(id: str) -> bool:
    if not get_by_id(id):
        return True
    return bool(
        session.query(CognitionIntegration)
        .filter(
            CognitionIntegration.id == id,
            CognitionIntegration.state.in_(FINISHED_STATES),
        )
        .first()
    )


def delete_many(
    ids: List[str], delete_cognition_groups: bool = True, with_commit: bool = True
) -> None:
    (
        session.query(CognitionIntegration)
        .filter(CognitionIntegration.id.in_(ids))
        .delete(synchronize_session=False)
    )
    if delete_cognition_groups:
        (
            session.query(CognitionGroup)
            .filter(CognitionGroup.meta_data.op("->>")("integration_id").in_(ids))
            .delete(synchronize_session=False)
        )
    general.flush_or_commit(with_commit)


def get_sharepoint_permissions_by_integration_id(
    integration_id: str,
) -> Dict[str, Any]:
    integration_id = prevent_sql_injection(
        integration_id, isinstance(integration_id, str)
    )
    query = f"""SELECT permission_id, object_id
    FROM (
    SELECT json_array_elements_text(permissions) permission_id, MAX(id::TEXT)::UUID id
    FROM integration.sharepoint
    WHERE integration_id = '{integration_id}'
    GROUP BY 1 
    )x
    INNER JOIN integration.sharepoint s
        ON x.id = s.id
    """
    return session.execute(query).all()
