from typing import Any, List, Optional, Dict, Union
from sqlalchemy.orm.attributes import flag_modified

import datetime

from submodules.model import enums
from submodules.model.session import session
from submodules.model.business_objects import general
from submodules.model.models import (
    EtlTask,
    CognitionIntegration,
    IntegrationSharepoint,
)

FINISHED_STATES = [
    enums.CognitionMarkdownFileState.FINISHED.value,
    enums.CognitionMarkdownFileState.FAILED.value,
]


def get_by_ids(ids: List[str]) -> List[EtlTask]:
    return session.query(EtlTask).filter(EtlTask.id.in_(ids)).all()


def get_by_id(id: str) -> EtlTask:
    return session.query(EtlTask).filter(EtlTask.id == id).first()


def get_all(
    exclude_failed: Optional[bool] = False,
    only_active: Optional[bool] = False,
) -> List[EtlTask]:
    query = session.query(EtlTask)
    if exclude_failed:
        query = query.filter(
            EtlTask.state != enums.CognitionMarkdownFileState.FAILED.value
        )
    if only_active:
        query = query.filter(EtlTask.is_active == True)
    return query.order_by(EtlTask.created_at.desc()).all()


def get_all_in_org(
    org_id: str,
    exclude_failed: Optional[bool] = False,
    only_active: Optional[bool] = False,
) -> List[EtlTask]:
    query = session.query(EtlTask).filter(EtlTask.organization_id == org_id)
    if only_active:
        query = query.filter(EtlTask.is_active == True)
    if exclude_failed:
        query = query.filter(
            EtlTask.state != enums.CognitionMarkdownFileState.FAILED.value
        )
    return query.order_by(EtlTask.created_at.desc()).all()


def get_all_in_org_paginated(
    org_id: str,
    page: int = 1,
    page_size: int = 10,
) -> List[EtlTask]:
    query = session.query(EtlTask).filter(
        EtlTask.organization_id == org_id,
    )

    return (
        query.order_by(EtlTask.created_at.desc())
        .limit(page_size)
        .offset(max(0, (page - 1) * page_size))
        .all()
    )


def get_or_create_integration_etl_task(
    record: IntegrationSharepoint,
    org_id: Optional[str],
    integration: Optional[CognitionIntegration],
    original_file_name: Optional[str],
    file_path: Optional[str],
    full_config: Optional[Dict[str, Any]],
    priority: Optional[int] = -1,
) -> EtlTask:
    if etl_task := (
        session.query(EtlTask).filter(EtlTask.id == record.etl_task_id).first()
    ):
        return etl_task

    return create(
        org_id=org_id,
        user_id=integration.created_by,
        original_file_name=original_file_name,
        file_path=file_path,
        file_size_bytes=record.size,
        full_config=full_config,
        tokenizer=integration.tokenizer,
        priority=priority,
    )


def get_supported_extractors() -> Dict[str, List[str]]:
    extractors = {}
    for file_type in enums.ETLFileType:
        extractors[file_type.value] = file_type.get_supported_extractors()
    return extractors


def create(
    org_id: str,
    user_id: str,
    original_file_name: str,
    file_size_bytes: int,
    tokenizer: str,
    full_config: Dict[str, Any],
    priority: Optional[int] = -1,
    file_path: Optional[str] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
) -> EtlTask:
    etl_task: EtlTask = EtlTask(
        id=id,
        organization_id=org_id,
        created_by=user_id,
        original_file_name=original_file_name,
        file_path=file_path,
        file_size_bytes=file_size_bytes,
        full_config=full_config,
        tokenizer=tokenizer,
        priority=priority,
    )
    general.add(etl_task, with_commit)

    return etl_task


def update(
    id: Optional[str] = None,
    etl_task: Optional[EtlTask] = None,
    updated_by: Optional[str] = None,
    original_file_name: Optional[str] = None,
    file_path: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
    full_config: Optional[Dict] = None,
    started_at: Optional[datetime.datetime] = None,
    finished_at: Optional[Union[str, datetime.datetime]] = None,
    state: Optional[enums.CognitionMarkdownFileState] = None,
    is_active: Optional[bool] = None,
    priority: Optional[int] = None,
    error_message: Optional[str] = None,
    with_commit: bool = True,
) -> Optional[EtlTask]:
    if not id and not etl_task:
        return None
    if id:
        etl_task: EtlTask = get_by_id(id)
    if not etl_task:
        return None

    if updated_by is not None:
        etl_task.updated_by = updated_by
    if file_path is not None and etl_task.file_path is None:
        etl_task.file_path = file_path
    if file_size_bytes is not None and etl_task.file_size_bytes is None:
        etl_task.file_size_bytes = file_size_bytes
    if original_file_name is not None and etl_task.original_file_name is None:
        etl_task.original_file_name = original_file_name
    if full_config is not None:
        etl_task.full_config = full_config
        flag_modified(etl_task, "full_config")
    if started_at is not None:
        etl_task.started_at = started_at
    if finished_at is not None:
        if finished_at == "NULL":
            etl_task.finished_at = None
        else:
            etl_task.finished_at = finished_at
    if state is not None:
        etl_task.state = state.value
    if is_active is not None:
        etl_task.is_active = is_active
    if priority is not None:
        etl_task.priority = priority
    if error_message is not None:
        if error_message == "NULL":
            etl_task.error_message = None
        else:
            etl_task.error_message = error_message

    general.add(etl_task, with_commit)
    return etl_task


def execution_finished(id: str) -> bool:
    if not get_by_id(id):
        return True
    return bool(
        session.query(EtlTask)
        .filter(
            EtlTask.id == id,
            EtlTask.state.in_(FINISHED_STATES),
        )
        .first()
    )


def delete_many(ids: List[str], with_commit: bool = True) -> None:
    # TODO: cascade delete cached files
    (
        session.query(EtlTask)
        .filter(EtlTask.id.in_(ids))
        .delete(synchronize_session=False)
    )
    general.flush_or_commit(with_commit)
