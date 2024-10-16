from ..business_objects import general
from ..session import session
from ..models import FileTransformation, FileTransformationLLMLogs
from submodules.model import enums
from typing import Optional, List
from datetime import datetime


def get(
    org_id: str,
    file_extraction_id: str,
    transformation_key: str,
    only_completed: bool = False,
) -> FileTransformation:
    query = session.query(FileTransformation).filter(
        FileTransformation.organization_id == org_id,
        FileTransformation.file_extraction_id == file_extraction_id,
        FileTransformation.transformation_key == transformation_key,
    )

    if only_completed:
        query = query.filter(
            FileTransformation.state == enums.FileCachingState.COMPLETED.value
        )
    return query.first()


def get_by_id(org_id: str, file_transformation_id: str) -> FileTransformation:
    return (
        session.query(FileTransformation)
        .filter(
            FileTransformation.organization_id == org_id,
            FileTransformation.id == file_transformation_id,
        )
        .first()
    )


def get_all_by_file_extraction_id(
    org_id: str, file_extraction_id: str
) -> List[FileTransformation]:
    return (
        session.query(FileTransformation)
        .filter(
            FileTransformation.organization_id == org_id,
            FileTransformation.file_extraction_id == file_extraction_id,
        )
        .all()
    )


def create(
    org_id: str,
    file_extraction_id: str,
    transformation_key: str,
    minio_path: str,
    created_by: str,
    with_commit: bool = True,
) -> FileTransformation:

    file_transformation = FileTransformation(
        organization_id=org_id,
        file_extraction_id=file_extraction_id,
        minio_path=minio_path,
        transformation_key=transformation_key,
        bucket=org_id,
        created_by=created_by,
    )

    general.add(file_transformation, with_commit)

    return file_transformation


def update(
    org_id: str,
    file_transformation_id: str,
    minio_path: Optional[str] = None,
    state: Optional[str] = None,
    with_commit: bool = True,
) -> FileTransformation:
    file_transformation = get_by_id(org_id, file_transformation_id)
    if file_transformation.state == enums.FileCachingState.CANCELED.value:
        return
    if minio_path is not None:
        file_transformation.minio_path = minio_path
    if state is not None:
        file_transformation.state = state
    general.flush_or_commit(with_commit)
    return file_transformation


def delete(org_id: str, file_transformation_id: str, with_commit: bool = True):
    file_transformation = get_by_id(org_id, file_transformation_id)
    general.delete(file_transformation, with_commit)


def set_state_to_failed(
    org_id: str, file_transformation_id: str, with_commit: bool = True
) -> FileTransformation:
    file_transformation = get_by_id(org_id, file_transformation_id)
    if (
        not file_transformation
        or file_transformation.state == enums.FileCachingState.CANCELED.value
        or file_transformation.state == enums.FileCachingState.COMPLETED.value
    ):
        return
    file_transformation.state = enums.FileCachingState.FAILED.value
    general.flush_or_commit(with_commit)
    return file_transformation


def set_state_to_failed_by_transformation_key(
    org_id: str,
    file_extraction_id: str,
    transformation_key: str,
    with_commit: bool = True,
) -> FileTransformation:
    file_transformation = get(org_id, file_extraction_id, transformation_key)
    if (
        not file_transformation
        or file_transformation.state == enums.FileCachingState.CANCELED.value
        or file_transformation.state == enums.FileCachingState.COMPLETED.value
    ):
        return
    file_transformation.state = enums.FileCachingState.FAILED.value
    general.flush_or_commit(with_commit)
    return file_transformation


def delete_all_by_file_extraction_id(
    org_id: str, file_extraction_id: str, with_commit: bool = True
):
    session.query(FileTransformation).filter(
        FileTransformation.organization_id == org_id,
        FileTransformation.file_extraction_id == file_extraction_id,
    ).delete()
    general.flush_or_commit(with_commit)


def create_file_transformation_llm_log(
    file_transformation_id: str,
    model_used: str,
    input_text: str,
    output_text: Optional[str] = None,
    error: Optional[str] = None,
    created_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    with_commit: bool = True,
) -> None:
    file_transformation_llm_log = FileTransformationLLMLogs(
        file_transformation_id=file_transformation_id,
        input=input_text,
        output=output_text,
        error=error,
        created_at=created_at,
        finished_at=finished_at,
        model_used=model_used,
    )
    general.add(file_transformation_llm_log, with_commit)

    return file_transformation_llm_log


def get_llm_logs_count(org_id: str, file_transformation_id: str) -> int:
    return (
        session.query(FileTransformationLLMLogs)
        .filter(
            FileTransformationLLMLogs.file_transformation_id == file_transformation_id
        )
        .count()
    )
