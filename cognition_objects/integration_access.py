from typing import List, Optional
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionOrganizationIntegrationAccess
from ..enums import CognitionThirdPartyIntegrationType


def get_by_id(id: str) -> CognitionOrganizationIntegrationAccess:
    return (
        session.query(CognitionOrganizationIntegrationAccess)
        .filter(CognitionOrganizationIntegrationAccess.id == id)
        .first()
    )


def get_by_org_id(org_id: str) -> List[CognitionOrganizationIntegrationAccess]:
    return (
        session.query(CognitionOrganizationIntegrationAccess)
        .filter(CognitionOrganizationIntegrationAccess.organization_id == org_id)
        .all()
    )


def get(
    org_id: str, integration_type: CognitionThirdPartyIntegrationType
) -> List[CognitionOrganizationIntegrationAccess]:
    return (
        session.query(CognitionOrganizationIntegrationAccess)
        .filter(
            CognitionOrganizationIntegrationAccess.organization_id == org_id,
            CognitionOrganizationIntegrationAccess.integration_type == integration_type,
        )
        .order_by(CognitionOrganizationIntegrationAccess.created_at.asc())
        .all()
    )


def create(
    org_id: str,
    user_id: str,
    integration_type: CognitionThirdPartyIntegrationType,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionOrganizationIntegrationAccess:
    integration_access: CognitionOrganizationIntegrationAccess = (
        CognitionOrganizationIntegrationAccess(
            org_id=org_id,
            created_by=user_id,
            created_at=created_at,
            integration_type=integration_type,
        )
    )
    general.add(integration_access, with_commit)

    return integration_access


def delete(id: str, with_commit: bool = True) -> None:
    session.query(CognitionOrganizationIntegrationAccess).filter(
        CognitionOrganizationIntegrationAccess.id == id
    ).delete()
    general.flush_or_commit(with_commit)
