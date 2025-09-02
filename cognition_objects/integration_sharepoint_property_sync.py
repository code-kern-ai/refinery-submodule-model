from typing import List, Optional, Dict, Any
from sqlalchemy.orm.attributes import flag_modified
from ..business_objects import general
from ..session import session
from ..models import IntegrationSharepointPropertySync


def get_by_integration_id(
    integration_id: str,
) -> List[IntegrationSharepointPropertySync]:
    return (
        session.query(IntegrationSharepointPropertySync)
        .filter(IntegrationSharepointPropertySync.integration_id == integration_id)
        .first()
    )


def create(
    integration_id: str,
    config: Optional[Dict[str, Any]] = None,
    logs: Optional[List[Dict[str, Any]]] = None,
    with_commit: bool = True,
) -> IntegrationSharepointPropertySync:
    integration_sync = IntegrationSharepointPropertySync(
        integration_id=integration_id,
        config=config or {},
        logs=logs or [],
    )
    session.add(integration_sync)
    general.flush_or_commit(with_commit)
    return integration_sync


def update(
    integration_id: str,
    config: Optional[Dict[str, Any]] = None,
    logs: Optional[List[Dict[str, Any]]] = None,
    with_commit: bool = True,
) -> IntegrationSharepointPropertySync:
    integration_sync = get_by_integration_id(integration_id)
    if config is not None:
        integration_sync.config = config
        flag_modified(integration_sync, "config")
    if logs is not None:
        integration_sync.logs = logs
        flag_modified(integration_sync, "logs")
    general.flush_or_commit(with_commit)
    return integration_sync
