from typing import List, Optional
from datetime import datetime

from ..models import IntegrationPdf
from .. import integration_objects


def get_by_id(id: str) -> IntegrationPdf:
    return integration_objects.get_by_id(IntegrationPdf, id)


def get_by_running_id(integration_id: str, running_id: int) -> IntegrationPdf:
    return integration_objects.get_by_running_id(
        IntegrationPdf, integration_id, running_id
    )


def get_all_by_integration_id(integration_id: str) -> List[IntegrationPdf]:
    return integration_objects.get_all_by_integration_id(IntegrationPdf, integration_id)


def get_all_by_project_id(project_id: str) -> List[IntegrationPdf]:
    return integration_objects.get_all_by_project_id(IntegrationPdf, project_id)


def create(
    created_by: str,
    integration_id: str,
    running_id: int,
    source: str,
    file_path: str,
    page: int,
    total_pages: int,
    title: str,
    delta_criteria: str,
    minio_file_name: str,
    created_at: Optional[datetime] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
) -> IntegrationPdf:
    return integration_objects.create(
        IntegrationPdf,
        created_by=created_by,
        integration_id=integration_id,
        running_id=running_id,
        source=source,
        file_path=file_path,
        page=page,
        total_pages=total_pages,
        title=title,
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
    file_path: Optional[str] = None,
    page: Optional[int] = None,
    total_pages: Optional[int] = None,
    title: Optional[str] = None,
    delta_criteria: Optional[str] = None,
    minio_file_name: Optional[str] = None,
    updated_at: Optional[datetime] = None,
    with_commit: bool = True,
) -> IntegrationPdf:
    return integration_objects.update(
        IntegrationPdf,
        id=id,
        updated_by=updated_by,
        running_id=running_id,
        source=source,
        file_path=file_path,
        page=page,
        total_pages=total_pages,
        title=title,
        delta_criteria=delta_criteria,
        minio_file_name=minio_file_name,
        updated_at=updated_at,
        with_commit=with_commit,
    )


def clear_history(id: str, with_commit: bool = True) -> None:
    integration_objects.clear_history(IntegrationPdf, id, with_commit)


def delete_many(ids: List[str], with_commit: bool = True) -> None:
    integration_objects.delete_many(IntegrationPdf, ids, with_commit)
