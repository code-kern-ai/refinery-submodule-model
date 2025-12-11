from typing import Any, List, Optional, Dict, Tuple, Union
from sqlalchemy.sql.expression import cast
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.dialects.postgresql import UUID

import datetime
import mimetypes

from submodules.model import enums
from submodules.model.session import session
from submodules.model.business_objects import general
from submodules.model.models import (
    EtlTask,
    CognitionIntegration,
    IntegrationSharepoint,
)
from submodules.model.util import prevent_sql_injection
from submodules.model.etl_utils import get_hashed_string


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
        meta_data={"integration_id": str(integration.id)},
        priority=priority,
    )


def get_supported_file_extensions() -> Dict[str, List[str]]:
    file_extensions = {}
    for file_type in enums.ETLFileType:
        file_extensions[file_type.value] = file_type.get_supported_file_extensions()
    file_extensions[enums.ETLFileType.TXT.value].append(
        [
            ext[0]
            for ext in filter(
                lambda x: x[1].startswith("text/"), mimetypes.types_map.items()
            )
        ]
    )
    return file_extensions


def get_or_create(
    org_id: str,
    user_id: str,
    original_file_name: str,
    file_size_bytes: int,
    tokenizer: Optional[str] = None,
    full_config: Optional[Dict[str, Any]] = None,
    file_path: Optional[str] = None,
    meta_data: Optional[Dict[str, Any]] = None,
    priority: Optional[int] = -1,
    id: Optional[str] = None,
    with_commit: bool = True,
) -> Tuple[EtlTask, bool]:
    if id:
        return get_by_id(id)

    query: EtlTask = session.query(EtlTask).filter(
        EtlTask.organization_id == org_id,
        EtlTask.original_file_name == original_file_name,
        EtlTask.file_size_bytes == file_size_bytes,
    )

    if file_path:
        query = query.filter(EtlTask.file_path == file_path)

    file_reference_id = meta_data.get("file_reference_id") if meta_data else None
    integration_id = meta_data.get("integration_id") if meta_data else None
    markdown_file_id = meta_data.get("markdown_file_id") if meta_data else None
    if file_reference_id:
        query = query.filter(
            file_reference_id
            == cast(EtlTask.meta_data.op("->>")("file_reference_id"), UUID)
        )
    if markdown_file_id:
        query = query.filter(
            markdown_file_id
            == cast(EtlTask.meta_data.op("->>")("markdown_file_id"), UUID)
        )
    if integration_id:
        query = query.filter(
            integration_id == cast(EtlTask.meta_data.op("->>")("integration_id"), UUID)
        )

    if etl_task := query.first():
        return etl_task, True

    return (
        create(
            org_id=org_id,
            user_id=user_id,
            original_file_name=original_file_name,
            file_size_bytes=file_size_bytes,
            tokenizer=tokenizer,
            full_config=full_config,
            meta_data=meta_data,
            priority=priority,
            file_path=file_path,
            id=id,
            with_commit=with_commit,
        ),
        False,
    )


def create(
    org_id: str,
    user_id: str,
    original_file_name: str,
    file_size_bytes: int,
    tokenizer: str,
    full_config: Dict[str, Any],
    meta_data: Optional[Dict[str, Any]] = None,
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
        tokenizer=tokenizer,
        full_config=full_config,
        full_config_hash=get_hashed_string(full_config),
        meta_data=meta_data,
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
    meta_data: Optional[Dict[str, Any]] = None,
    priority: Optional[int] = None,
    error_message: Optional[str] = None,
    overwrite_meta_data: bool = False,
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
    if meta_data is not None:
        if overwrite_meta_data:
            etl_task.meta_data = meta_data
        else:
            etl_task.meta_data.update(meta_data)
        flag_modified(etl_task, "meta_data")
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


def get_last_etl_tasks(
    states: List[enums.CognitionMarkdownFileState],
    created_at_from: str,
    created_at_to: Optional[str] = None,
) -> List[Any]:

    states = [state.value for state in states]
    if len(states) == 0:
        return []

    created_at_from = prevent_sql_injection(
        created_at_from, isinstance(created_at_from, str)
    )
    if created_at_to:
        created_at_to = prevent_sql_injection(
            created_at_to, isinstance(created_at_to, str)
        )
    created_at_to_filter = ""

    if created_at_to:
        created_at_to_filter = f"AND mf.created_at <= '{created_at_to}'"

    states_filter_sql = ", ".join([f"'{state}'" for state in states])

    query = f"""
    SELECT *
    FROM (
        SELECT 
            et.organization_id, 
            et.created_at, 
            et.created_by, 
            et.started_at, 
            et.finished_at, 
            et.original_file_name AS file_name, 
            et.error_message AS error, 
            et.state, 
            md.id AS dataset_id, 
            md.name AS dataset_name,
            ig.id AS integration_id,
            ig.name AS integration_name,
            o.name AS organization_name,
            ROW_NUMBER() OVER (
                PARTITION BY md.organization_id, md.id
                ORDER BY et.created_at DESC
            ) AS rn
        FROM global.etl_task et
        JOIN organization o ON o.id = et.organization_id
        LEFT JOIN cognition.markdown_file mf ON et.id = mf.etl_task_id
        LEFT JOIN cognition.markdown_dataset md ON md.id = mf.dataset_id
        LEFT JOIN cognition.integration ig ON et.meta_data->>'integration_id' = ig.id::TEXT
        WHERE 
            et.created_at >= '{created_at_from}'
            AND et.state IN ({states_filter_sql})
            {created_at_to_filter}
    ) sub
    WHERE sub.rn <= 5
    ORDER BY organization_id, dataset_id, created_at DESC
    """

    return general.execute_all(query)
