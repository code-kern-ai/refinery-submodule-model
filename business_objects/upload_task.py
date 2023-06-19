import datetime
from typing import Optional, Dict, Any

from ..exceptions import EntityNotFoundException
from ..session import session
from ..models import UploadTask
from ..business_objects import general


def get(project_id: str, task_id: str) -> UploadTask:
    return (
        session.query(UploadTask)
        .filter(
            UploadTask.project_id == project_id,
            UploadTask.id == task_id,
        )
        .first()
    )


def get_with_file_name(
    project_id: str, upload_task_id: str, file_name: str
) -> UploadTask:
    query = session.query(UploadTask).filter(
        UploadTask.id == upload_task_id,
        UploadTask.project_id == project_id,
        UploadTask.file_name == file_name,
    )
    task = query.first()
    if not task:
        raise EntityNotFoundException("Upload task not found")
    else:
        return task


def create(
    user_id: str,
    project_id: str,
    file_name: str,
    file_type: str,
    file_import_options: str,
    upload_type: str,
    password: str,
    with_commit: bool = False,
) -> UploadTask:
    task = UploadTask(
        user_id=user_id,
        project_id=project_id,
        file_name=file_name,
        file_type=file_type,
        file_import_options=file_import_options,
        upload_type=upload_type,
        password=password,
    )
    general.add(task, with_commit)
    return task


def update(
    project_id: str,
    task_id: str,
    state: Optional[str] = None,
    progress: Optional[float] = None,
    file_additional_info: str = None,
    mappings: Optional[Dict[str, Any]] = None,
    with_commit: bool = False,
) -> None:
    task: UploadTask = get(project_id, task_id)
    if state is not None:
        task.state = state
    if progress is not None:
        task.progress = progress
    if file_additional_info is not None:
        task.file_additional_info = file_additional_info
    if mappings is not None:
        task.mappings = mappings
    general.flush_or_commit(with_commit)


def finish(project_id: str, task_id: str, with_commit: bool = False) -> None:
    task: UploadTask = get(project_id, task_id)
    task.finished_at = datetime.datetime.now()
    general.flush_or_commit(with_commit)
