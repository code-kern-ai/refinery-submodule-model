from datetime import datetime
from typing import List, Optional
from ..business_objects import general
from ..session import session
from ..models import CognitionRefinerySynchronizationTask
from .. import enums


def get(project_id: str, task_id: str) -> CognitionRefinerySynchronizationTask:
    return (
        session.query(CognitionRefinerySynchronizationTask)
        .filter(
            CognitionRefinerySynchronizationTask.cognition_project_id == project_id,
            CognitionRefinerySynchronizationTask.id == task_id,
        )
        .first()
    )


def get_all_by_project_id(
    project_id: str,
) -> List[CognitionRefinerySynchronizationTask]:
    return (
        session.query(CognitionRefinerySynchronizationTask)
        .filter(CognitionRefinerySynchronizationTask.cognition_project_id == project_id)
        .order_by(CognitionRefinerySynchronizationTask.created_at.asc())
        .all()
    )


def get_last_by_project_id(project_id: str) -> CognitionRefinerySynchronizationTask:
    return (
        session.query(CognitionRefinerySynchronizationTask)
        .filter(CognitionRefinerySynchronizationTask.cognition_project_id == project_id)
        .order_by(CognitionRefinerySynchronizationTask.created_at.desc())
        .first()
    )


def create(
    project_id: str,
    refinery_project_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionRefinerySynchronizationTask:
    task = CognitionRefinerySynchronizationTask(
        cognition_project_id=project_id,
        refinery_project_id=refinery_project_id,
        created_at=created_at,
        state=enums.RefinerySynchronizationTaskState.CREATED.value,
        logs=[],
    )

    general.add(task, with_commit)

    return task


def update(
    project_id: str,
    task_id: str,
    state: Optional[str] = None,
    finished_at: Optional[datetime] = None,
    logs: Optional[List[str]] = None,
    num_records_created: Optional[int] = None,
    with_commit: bool = True,
) -> CognitionRefinerySynchronizationTask:
    task = get(project_id=project_id, task_id=task_id)

    if state is not None:
        task.state = state

    if finished_at is not None:
        task.finished_at = finished_at

    if logs is not None:
        task.logs = logs

    if num_records_created is not None:
        task.num_records_created = num_records_created

    general.flush_or_commit(with_commit)

    return task
