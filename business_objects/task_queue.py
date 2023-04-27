from typing import List, Optional, Dict, Set
from sqlalchemy import text

from . import general
from .. import enums
from ..models import TaskQueue
from ..session import session


def get(task_id: str) -> Optional[TaskQueue]:
    return session.query(TaskQueue).filter(TaskQueue.id == task_id).first()


def get_all_tasks() -> List[TaskQueue]:
    return session.query(TaskQueue).order_by(TaskQueue.created_at.asc()).all()


def get_all_waiting_by_type(project_id: str, type: enums.TaskType) -> List[TaskQueue]:
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.project_id == project_id,
            TaskQueue.type == type.value,
            TaskQueue.is_active == False,
        )
        .order_by(TaskQueue.created_at.asc())
        .all()
    )


def get_waiting_by_attribute_id(project_id: str, attribute_id: str) -> TaskQueue:
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.project_id == project_id,
            TaskQueue.type == enums.TaskType.ATTRIBUTE_CALCULATION.value,
            text(f"task_info->>'attribute_id' = '{attribute_id}'"),
            TaskQueue.is_active == False,
        )
        .first()
    )


def get_waiting_by_information_source(project_id: str, source_id: str) -> TaskQueue:
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.project_id == project_id,
            TaskQueue.type == enums.TaskType.INFORMATION_SOURCE.value,
            text(f"task_info->>'information_source_id' = '{source_id}'"),
            TaskQueue.is_active == False,
        )
        .first()
    )


def get_by_tokenization(project_id: str) -> TaskQueue:
    # could have multiple tokenization tasks in queue
    # if active => something is running else it's queued
    return (
        session.query(TaskQueue)
        .filter(
            TaskQueue.project_id == project_id,
            TaskQueue.type == enums.TaskType.TOKENIZATION.value,
        )
        .order_by(TaskQueue.created_at.asc())
        .first()
    )


def add(
    project_id: str,
    type: enums.TaskType,
    created_by: str,
    task_info: Dict[str, str],
    priority: bool,
    with_commit: bool = False,
) -> TaskQueue:
    tbl_entry = TaskQueue(
        project_id=project_id,
        type=type.value,
        created_by=created_by,
        task_info=task_info,
        priority=priority,
    )
    general.add(tbl_entry, with_commit)
    return tbl_entry


def set_task_active(project_id: str, task_id: str, with_commit: bool = False):
    session.query(TaskQueue).filter(
        TaskQueue.id == task_id,
        TaskQueue.project_id == project_id,
    ).update({"is_active": True})
    general.flush_or_commit(with_commit)


def set_all_tasks_inactive(with_commit: bool = False):
    session.query(TaskQueue).filter(
        TaskQueue.is_active == True,
    ).update({"is_active": False})
    general.flush_or_commit(with_commit)


def remove_task_from_queue(project_id: str, task_id: str, with_commit: bool = False):
    session.query(TaskQueue).filter(
        TaskQueue.id == task_id,
        TaskQueue.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
