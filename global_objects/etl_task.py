from typing import List, Optional, Dict, Union
from sqlalchemy.orm.attributes import flag_modified

import datetime

from ..business_objects import general
from ..session import session
from ..models import EtlTask
from ..enums import CognitionMarkdownFileState

FINISHED_STATES = [
    CognitionMarkdownFileState.FINISHED.value,
    CognitionMarkdownFileState.FAILED.value,
]


def get_by_ids(ids: List[str]) -> List[EtlTask]:
    return session.query(EtlTask).filter(EtlTask.id.in_(ids)).all()


def get_by_id(id: str) -> EtlTask:
    return session.query(EtlTask).filter(EtlTask.id == id).first()


def get_all(
    markdown_file_id: Optional[str] = None,
    sharepoint_file_id: Optional[str] = None,
    exclude_failed: Optional[bool] = False,
    only_active: Optional[bool] = False,
) -> List[EtlTask]:
    query = session.query(EtlTask)
    if markdown_file_id is not None and sharepoint_file_id is not None:
        raise ValueError(
            "get_all: Only one of markdown_file_id or sharepoint_file_id should be provided."
        )
    if markdown_file_id:
        query = query.filter(EtlTask.markdown_file_id == markdown_file_id)
    if sharepoint_file_id:
        query = query.filter(EtlTask.sharepoint_file_id == sharepoint_file_id)

    if exclude_failed:
        query = query.filter(EtlTask.state != CognitionMarkdownFileState.FAILED.value)
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
        query = query.filter(EtlTask.state != CognitionMarkdownFileState.FAILED.value)
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


def create(
    org_id: str,
    user_id: str,
    file_size_bytes: int,
    cache_config: Dict,
    extract_config: Dict,
    split_config: Dict,
    transform_config: Dict,
    load_config: Dict,
    notify_config: Dict,
    llm_config: Dict,
    tokenizer: str,
    priority: Optional[int] = -1,
    file_path: Optional[str] = None,
    id: Optional[str] = None,
    with_commit: bool = True,
) -> EtlTask:
    etl_task: EtlTask = EtlTask(
        id=id,
        organization_id=org_id,
        created_by=user_id,
        file_path=file_path,
        file_size_bytes=file_size_bytes,
        cache_config=cache_config,
        extract_config=extract_config,
        split_config=split_config,
        transform_config=transform_config,
        load_config=load_config,
        notify_config=notify_config,
        llm_config=llm_config,
        tokenizer=tokenizer,
        priority=priority,
    )
    general.add(etl_task, with_commit)

    return etl_task


def update(
    id: Optional[str] = None,
    etl_task: Optional[EtlTask] = None,
    updated_by: Optional[str] = None,
    file_path: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
    cache_config: Optional[Dict] = None,
    extract_config: Optional[Dict] = None,
    split_config: Optional[Dict] = None,
    transform_config: Optional[Dict] = None,
    load_config: Optional[Dict] = None,
    notify_config: Optional[Dict] = None,
    llm_config: Optional[Dict] = None,
    started_at: Optional[datetime.datetime] = None,
    finished_at: Optional[Union[str, datetime.datetime]] = None,
    state: Optional[CognitionMarkdownFileState] = None,
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
    if cache_config is not None:
        etl_task.cache_config = cache_config
        flag_modified(etl_task, "cache_config")
    if extract_config is not None:
        etl_task.extract_config = extract_config
        flag_modified(etl_task, "extract_config")
    if split_config is not None:
        etl_task.split_config = split_config
        flag_modified(etl_task, "split_config")
    if transform_config is not None:
        etl_task.transform_config = transform_config
        flag_modified(etl_task, "transform_config")
    if load_config is not None:
        etl_task.load_config = load_config
        flag_modified(etl_task, "load_config")
    if notify_config is not None:
        etl_task.notify_config = notify_config
        flag_modified(etl_task, "notify_config")
    if llm_config is not None:
        etl_task.llm_config = llm_config
        flag_modified(etl_task, "llm_config")
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
    (
        session.query(EtlTask)
        .filter(EtlTask.id.in_(ids))
        .delete(synchronize_session=False)
    )
    general.flush_or_commit(with_commit)
