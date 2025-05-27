from typing import List, Optional
from datetime import datetime

from ..models import IntegrationGithubFile
from .. import integration_objects


def get_by_id(id: str) -> IntegrationGithubFile:
    return integration_objects.get_by_id(IntegrationGithubFile, id)


def get_by_running_id(integration_id: str, running_id: int) -> IntegrationGithubFile:
    return integration_objects.get_by_running_id(
        IntegrationGithubFile, integration_id, running_id
    )


def get_all_by_integration_id(integration_id: str) -> List[IntegrationGithubFile]:
    return integration_objects.get_all_by_integration_id(
        IntegrationGithubFile, integration_id
    )


def get_all_by_project_id(project_id: str) -> List[IntegrationGithubFile]:
    return integration_objects.get_all_by_project_id(IntegrationGithubFile, project_id)


def create(
    created_by: str,
    integration_id: str,
    running_id: int,
    source: str,
    path: str,
    sha: str,
    delta_criteria: str,
    minio_file_name: str,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
) -> IntegrationGithubFile:
    return integration_objects.create(
        IntegrationGithubFile,
        created_by=created_by,
        integration_id=integration_id,
        running_id=running_id,
        source=source,
        path=path,
        sha=sha,
        delta_criteria=delta_criteria,
        minio_file_name=minio_file_name,
        created_at=created_at,
        id=id,
        with_commit=with_commit,
    )


def update(
    id: str,
    updated_by: str,
    running_id: Optional[int] = None,
    source: Optional[str] = None,
    path: Optional[str] = None,
    sha: Optional[str] = None,
    delta_criteria: Optional[str] = None,
    minio_file_name: Optional[str] = None,
    updated_at: Optional[datetime] = None,
    with_commit: bool = True,
) -> IntegrationGithubFile:
    return integration_objects.update(
        IntegrationGithubFile,
        id=id,
        updated_by=updated_by,
        running_id=running_id,
        source=source,
        path=path,
        sha=sha,
        delta_criteria=delta_criteria,
        minio_file_name=minio_file_name,
        updated_at=updated_at,
        with_commit=with_commit,
    )


def clear_history(id: str, with_commit: bool = True) -> None:
    integration_objects.clear_history(IntegrationGithubFile, id, with_commit)


def delete_many(ids: List[str], with_commit: bool = True) -> None:
    integration_objects.delete_many(IntegrationGithubFile, ids, with_commit)
