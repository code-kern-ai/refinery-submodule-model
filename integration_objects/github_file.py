from typing import List, Optional
from datetime import datetime

from ..models import IntegrationGithubFile
from .. import integration_objects

IntegrationModel = IntegrationGithubFile


def get_by_id(id: str) -> IntegrationModel:
    return integration_objects.get_by_id(IntegrationModel, id)


def get_by_running_id(integration_id: str, running_id: int) -> IntegrationModel:
    return integration_objects.get_by_running_id(
        IntegrationModel, integration_id, running_id
    )


def get_all_by_integration_id(integration_id: str) -> List[IntegrationModel]:
    return integration_objects.get_all_by_integration_id(
        IntegrationModel, integration_id
    )


def get_all_by_project_id(project_id: str) -> List[IntegrationModel]:
    return integration_objects.get_all_by_project_id(IntegrationModel, project_id)


def create(
    created_by: str,
    integration_id: str,
    running_id: int,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
    **metadata
) -> IntegrationModel:
    return integration_objects.create(
        IntegrationModel,
        created_by=created_by,
        integration_id=integration_id,
        running_id=running_id,
        created_at=created_at,
        id=id,
        with_commit=with_commit,
        **metadata
    )


def update(
    id: str,
    updated_by: str,
    running_id: Optional[int] = None,
    updated_at: Optional[datetime] = None,
    **metadata
) -> IntegrationModel:
    return integration_objects.update(
        IntegrationModel,
        id=id,
        updated_by=updated_by,
        running_id=running_id,
        updated_at=updated_at,
        **metadata
    )


def clear_history(id: str, with_commit: bool = True) -> None:
    integration_objects.clear_history(IntegrationModel, id, with_commit)


def delete_many(ids: List[str], with_commit: bool = True) -> None:
    integration_objects.delete_many(IntegrationModel, ids, with_commit)
