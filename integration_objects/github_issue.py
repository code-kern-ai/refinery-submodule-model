from typing import List, Optional
from datetime import datetime

from ..models import IntegrationGithubIssue
from .. import integration_objects


def get_by_id(id: str) -> IntegrationGithubIssue:
    return integration_objects.get_by_id(IntegrationGithubIssue, id)


def get_by_running_id(integration_id: str, running_id: int) -> IntegrationGithubIssue:
    return integration_objects.get_by_running_id(
        IntegrationGithubIssue, integration_id, running_id
    )


def get_all_by_integration_id(integration_id: str) -> List[IntegrationGithubIssue]:
    return integration_objects.get_all_by_integration_id(
        IntegrationGithubIssue, integration_id
    )


def get_all_by_project_id(project_id: str) -> List[IntegrationGithubIssue]:
    return integration_objects.get_all_by_project_id(IntegrationGithubIssue, project_id)


def create(
    created_by: str,
    integration_id: str,
    running_id: int,
    source: str,
    url: str,
    state: str,
    number: str,
    delta_criteria: str,
    minio_file_name: str,
    milestone: Optional[str] = None,
    assignee: Optional[str] = None,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
) -> IntegrationGithubIssue:
    return integration_objects.create(
        IntegrationGithubIssue,
        created_by=created_by,
        integration_id=integration_id,
        running_id=running_id,
        source=source,
        url=url,
        state=state,
        number=number,
        milestone=milestone,
        assignee=assignee,
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
    url: Optional[str] = None,
    state: Optional[str] = None,
    number: Optional[str] = None,
    milestone: Optional[str] = None,
    assignee: Optional[str] = None,
    delta_criteria: Optional[str] = None,
    minio_file_name: Optional[str] = None,
    updated_at: Optional[datetime] = None,
    with_commit: bool = True,
) -> IntegrationGithubIssue:
    return integration_objects.update(
        IntegrationGithubIssue,
        id=id,
        updated_by=updated_by,
        running_id=running_id,
        source=source,
        url=url,
        state=state,
        number=number,
        milestone=milestone,
        assignee=assignee,
        delta_criteria=delta_criteria,
        minio_file_name=minio_file_name,
        updated_at=updated_at,
        with_commit=with_commit,
    )


def clear_history(id: str, with_commit: bool = True) -> None:
    integration_objects.clear_history(IntegrationGithubIssue, id, with_commit)


def delete_many(ids: List[str], with_commit: bool = True) -> None:
    integration_objects.delete_many(IntegrationGithubIssue, ids, with_commit)
