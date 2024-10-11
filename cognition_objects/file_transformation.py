from ..business_objects import general
from ..session import session
from ..models import FileTransformation
from submodules.model import enums


def get(
    org_id: str,
    file_extraction_id: str,
    transformation_key: str,
    only_completed: bool = False,
) -> FileTransformation:
    return (
        session.query(FileTransformation)
        .filter(
            FileTransformation.organization_id == org_id,
            FileTransformation.file_extraction_id == file_extraction_id,
            FileTransformation.transformation_key == transformation_key,
            (
                FileTransformation.state == enums.FileCachingState.COMPLETED.value
                if only_completed
                else True
            ),
        )
        .first()
    )


def get_by_id(org_id, file_transformation_id: str) -> FileTransformation:
    return (
        session.query(FileTransformation)
        .filter(
            FileTransformation.organization_id == org_id,
            FileTransformation.id == file_transformation_id,
        )
        .first()
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
    minio_path: str = None,
    state: str = None,
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
