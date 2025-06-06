from typing import List, Optional
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionIntegrationAccess
from ..enums import CognitionIntegrationType


def get_by_id(id: str) -> CognitionIntegrationAccess:
    return (
        session.query(CognitionIntegrationAccess)
        .filter(CognitionIntegrationAccess.id == id)
        .first()
    )


def get_by_org_id(org_id: str) -> List[CognitionIntegrationAccess]:
    return (
        session.query(CognitionIntegrationAccess)
        .filter(CognitionIntegrationAccess.organization_id == org_id)
        .all()
    )


def get(
    org_id: str, integration_type: CognitionIntegrationType
) -> List[CognitionIntegrationAccess]:
    return (
        session.query(CognitionIntegrationAccess)
        .filter(
            CognitionIntegrationAccess.organization_id == org_id,
            CognitionIntegrationAccess.integration_type == integration_type,
        )
        .order_by(CognitionIntegrationAccess.created_at.asc())
        .all()
    )


def create(
    org_id: str,
    user_id: str,
    integration_types: List[str],
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionIntegrationAccess:
    integration_access: CognitionIntegrationAccess = CognitionIntegrationAccess(
        organization_id=org_id,
        created_by=user_id,
        created_at=created_at,
        integration_types=integration_types,
    )
    general.add(integration_access, with_commit)

    return integration_access


def update(
    id: str,
    org_id: Optional[str] = None,
    integration_types: Optional[List[str]] = None,
    with_commit: bool = True,
) -> CognitionIntegrationAccess:
    integration_access = get_by_id(id)
    if org_id:
        integration_access.organization_id = org_id
    if integration_types:
        integration_access.integration_types = integration_types
    general.add(integration_access, with_commit)
    return integration_access


def delete(id: str, with_commit: bool = True) -> None:
    session.query(CognitionIntegrationAccess).filter(
        CognitionIntegrationAccess.id == id
    ).delete()
    general.flush_or_commit(with_commit)
