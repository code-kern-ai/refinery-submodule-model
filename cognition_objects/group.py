from datetime import datetime
from typing import List, Optional
from ..business_objects import general
from ..session import session
from ..models import CognitionGroup


def get(group_id: str) -> CognitionGroup:
    return session.query(CognitionGroup).filter(CognitionGroup.id == group_id).first()


def get_with_organization_id(organization_id: str, group_id: str) -> CognitionGroup:
    return (
        session.query(CognitionGroup)
        .filter(
            CognitionGroup.organization_id == organization_id,
            CognitionGroup.id == group_id,
        )
        .first()
    )


def get_all(organization_id: str) -> List[CognitionGroup]:
    return (
        session.query(CognitionGroup)
        .filter(CognitionGroup.organization_id == organization_id)
        .order_by(CognitionGroup.name.asc())
        .all()
    )


def get_all_by_integration_id_permission_grouped(
    organization_id: str, integration_id: str
) -> List[CognitionGroup]:
    integration_id_json = CognitionGroup.meta_data.op("->>")("integration_id")

    integration_groups = session.query(CognitionGroup).filter(CognitionGroup.organization_id == organization_id, integration_id_json == integration_id).all()
    integration_groups_by_permission = {}
    for group in integration_groups:
        permission_id = group.meta_data.get("permission_id")
        integration_groups_by_permission[permission_id] = group
    return integration_groups_by_permission


def create_group(
    organization_id: str,
    name: str,
    description: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
    meta_data: Optional[dict] = None,
) -> CognitionGroup:
    group = CognitionGroup(
        organization_id=organization_id,
        name=name,
        description=description,
        created_by=created_by,
        created_at=created_at,
        meta_data=meta_data,
    )
    general.add(group, with_commit)
    return group


def update_group(
    group_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = False,
) -> CognitionGroup:
    group = get(group_id)

    if name is not None:
        group.name = name
    if description is not None:
        group.description = description
    general.flush_or_commit(with_commit)

    return group


def delete(organization_id: str, group_id: str, with_commit: bool = False) -> None:
    group = get_with_organization_id(organization_id, group_id)
    if group:
        general.delete(group, with_commit)
