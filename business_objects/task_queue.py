from typing import List, Optional, Dict, Union
from sqlalchemy import text
from sqlalchemy.sql.expression import bindparam

from . import general
from .. import enums
from ..models import TaskQueue, Project
from ..session import session

from ..util import prevent_sql_injection
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql.expression import cast
from datetime import datetime, timedelta


def get(task_id: str) -> Optional[TaskQueue]:
    return session.query(TaskQueue).filter(TaskQueue.id == task_id).first()


def get_all_tasks() -> List[TaskQueue]:
    return session.query(TaskQueue).order_by(TaskQueue.created_at.asc()).all()


def get_orphan_tasks() -> List[TaskQueue]:
    task_project_id = cast(TaskQueue.task_info.op("->>")("project_id"), UUID)
    return (
        session.query(TaskQueue)
        .outerjoin(
            Project,
            task_project_id == Project.id,
        )
        .filter(task_project_id != None, Project.id == None)
        .all()
    )


def get_all_queued_etl_task_for_conversation(
    org_id: str, project_id: str, conversation_id: str
) -> Optional[List[TaskQueue]]:
    safe_project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    safe_conversation_id = prevent_sql_injection(
        conversation_id, isinstance(conversation_id, str)
    )
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.organization_id == org_id,
            TaskQueue.task_type == enums.TaskType.EXECUTE_ETL.value,
            text(
                "task_info->'tmp_doc_metadata'->>'project_id' = :project_id"
            ).bindparams(project_id=safe_project_id),
            text(
                "task_info->'tmp_doc_metadata'->>'conversation_id' = :conversation_id"
            ).bindparams(conversation_id=safe_conversation_id),
        )
        .all()
    )


def get_likely_failed_tasks(days: int = 1) -> List[TaskQueue]:
    return (
        session.query(TaskQueue)
        .filter(TaskQueue.created_at < datetime.now() - timedelta(days=days))
        .all()
    )


def get_all_waiting_by_type(
    project_id: str, task_type: enums.TaskType
) -> List[TaskQueue]:
    safe_project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return (
        session.query(TaskQueue)
        .filter(
            text("task_info->>'project_id' = :project_id").bindparams(
                project_id=safe_project_id
            ),
            TaskQueue.task_type == task_type.value,
            TaskQueue.is_active == False,
        )
        .order_by(TaskQueue.created_at.asc())
        .all()
    )


def get_waiting_by_attribute_id(project_id: str, attribute_id: str) -> TaskQueue:
    safe_project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    safe_attribute_id = prevent_sql_injection(
        attribute_id, isinstance(attribute_id, str)
    )
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.task_type == enums.TaskType.ATTRIBUTE_CALCULATION.value,
            text("task_info->>'attribute_id' = :attribute_id").bindparams(
                attribute_id=safe_attribute_id
            ),
            text("task_info->>'project_id' = :project_id").bindparams(
                project_id=safe_project_id
            ),
            TaskQueue.is_active == False,
        )
        .first()
    )


def get_waiting_by_information_source(project_id: str, source_id: str) -> TaskQueue:
    safe_source_id = prevent_sql_injection(source_id, isinstance(source_id, str))
    safe_project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.task_type == enums.TaskType.INFORMATION_SOURCE.value,
            text(
                "task_info->>'information_source_id' = :information_source_id"
            ).bindparams(information_source_id=safe_source_id),
            text("task_info->>'project_id' = :project_id").bindparams(
                project_id=safe_project_id
            ),
            TaskQueue.is_active == False,
        )
        .first()
    )


def get_waiting_by_macro_group_execution_ids(
    project_id: str, source_ids: List[str]
) -> TaskQueue:
    safe_source_ids = prevent_sql_injection(source_ids, isinstance(source_ids, list))
    safe_project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.task_type == enums.TaskType.RUN_COGNITION_MACRO.value,
            text(
                "task_info->>'group_execution_id' IN :source_ids"
            ).bindparams(bindparam("source_ids", safe_source_ids, expanding=True)),
            text("task_info->>'project_id' = :project_id").bindparams(
                project_id=safe_project_id
            ),
        )
        .first()
    )


def get_by_tokenization(project_id: str) -> TaskQueue:
    # could have multiple tokenization tasks in queue
    # if active => something is running else it's queued
    safe_project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.task_type == enums.TaskType.TOKENIZATION.value,
            text("task_info->>'project_id' = :project_id").bindparams(
                project_id=safe_project_id
            ),
        )
        .order_by(TaskQueue.created_at.asc())
        .first()
    )


def add(
    org_id: str,
    task_type: enums.TaskType,
    created_by: str,
    task_info: Union[List[Dict[str, str]], Dict[str, str]],
    priority: bool,
    with_commit: bool = False,
) -> TaskQueue:
    tbl_entry = TaskQueue(
        organization_id=org_id,
        task_type=task_type.value,
        created_by=created_by,
        task_info=task_info,
        priority=priority,
    )
    general.add(tbl_entry, with_commit)
    return tbl_entry


def set_task_active(org_id: str, task_id: str, with_commit: bool = False):
    session.query(TaskQueue).filter(
        TaskQueue.id == task_id,
        TaskQueue.organization_id == org_id,
    ).update({"is_active": True})
    general.flush_or_commit(with_commit)


def set_all_tasks_inactive(with_commit: bool = False):
    session.query(TaskQueue).filter(
        TaskQueue.is_active == True,
    ).update({"is_active": False})
    general.flush_or_commit(with_commit)


def update_task_info(
    task_id: str,
    task_info: Union[List[Dict[str, str]], Dict[str, str]],
    with_commit: bool = False,
):
    task_item = get(task_id)
    if not task_item:
        return
    if task_item.is_active and task_item.task_type != enums.TaskType.TASK_QUEUE.value:
        raise ValueError("can't update active tasks")
    task_item.task_info = task_info
    general.flush_or_commit(with_commit)


def remove_task_from_queue(org_id: str, task_id: str, with_commit: bool = False):
    session.query(TaskQueue).filter(
        TaskQueue.id == task_id,
        TaskQueue.organization_id == org_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_by_task_id(task_id: str, with_commit: bool = False):
    session.query(TaskQueue).filter(
        TaskQueue.id == task_id,
    ).delete()
    general.flush_or_commit(with_commit)
