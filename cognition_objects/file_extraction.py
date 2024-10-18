from ..business_objects import general
from ..session import session
from ..models import FileExtraction
from submodules.model import enums
from typing import Optional, List


def get(
    org_id: str,
    file_reference_id: str,
    extraction_key: str,
    only_completed: bool = False,
) -> FileExtraction:
    query = session.query(FileExtraction).filter(
        FileExtraction.organization_id == org_id,
        FileExtraction.file_reference_id == file_reference_id,
        FileExtraction.extraction_key == extraction_key,
    )

    if only_completed:
        query = query.filter(
            FileExtraction.state == enums.FileCachingState.COMPLETED.value
        )

    return query.first()


def get_by_id(org_id: str, file_extraction_id: str) -> FileExtraction:
    return (
        session.query(FileExtraction)
        .filter(
            FileExtraction.organization_id == org_id,
            FileExtraction.id == file_extraction_id,
        )
        .first()
    )


def get_all_by_file_reference_id(
    org_id: str, file_reference_id: str
) -> List[FileExtraction]:
    return (
        session.query(FileExtraction)
        .filter(
            FileExtraction.organization_id == org_id,
            FileExtraction.file_reference_id == file_reference_id,
        )
        .all()
    )


def get_all_by_extraction_key(org_id: str, extraction_key: str) -> List[FileExtraction]:
    return (
        session.query(FileExtraction)
        .filter(
            FileExtraction.organization_id == org_id,
            FileExtraction.extraction_key == extraction_key,
        )
        .all()
    )


def create(
    org_id: str,
    file_reference_id: str,
    extraction_key: str,
    minio_path: str,
    created_by: str,
    with_commit: bool = True,
) -> FileExtraction:

    file_extraction = FileExtraction(
        organization_id=org_id,
        file_reference_id=file_reference_id,
        minio_path=minio_path,
        extraction_key=extraction_key,
        bucket=org_id,
        created_by=created_by,
    )

    general.add(file_extraction, with_commit)

    return file_extraction


def update(
    org_id: str,
    file_extraction_id: str,
    minio_path: Optional[str] = None,
    state: Optional[str] = None,
    with_commit: bool = True,
) -> FileExtraction:
    file_extraction = get_by_id(org_id, file_extraction_id)
    if file_extraction.state == enums.FileCachingState.CANCELED.value:
        return
    if minio_path is not None:
        file_extraction.minio_path = minio_path
    if state is not None:
        file_extraction.state = state
    general.flush_or_commit(with_commit)
    return file_extraction


def delete(org_id: str, file_extraction_id: str, with_commit: bool = True) -> None:
    file_extraction = get_by_id(org_id, file_extraction_id)
    general.delete(file_extraction, with_commit)


def set_state_to_failed(
    org_id: str, file_extraction_id: str, with_commit: bool = True
) -> FileExtraction:
    file_extraction = get_by_id(org_id, file_extraction_id)
    if (
        not file_extraction
        or file_extraction.state == enums.FileCachingState.CANCELED.value
        or file_extraction.state == enums.FileCachingState.COMPLETED.value
    ):
        return
    file_extraction.state = enums.FileCachingState.FAILED.value
    general.flush_or_commit(with_commit)
    return file_extraction


def set_state_to_failed_by_extraction_key(
    org_id: str, file_reference_id: str, extraction_key: str, with_commit: bool = True
) -> FileExtraction:
    file_extraction = get(org_id, file_reference_id, extraction_key)
    if (
        not file_extraction
        or file_extraction.state == enums.FileCachingState.CANCELED.value
        or file_extraction.state == enums.FileCachingState.COMPLETED.value
    ):
        return
    file_extraction.state = enums.FileCachingState.FAILED.value
    general.flush_or_commit(with_commit)
    return file_extraction


def delete_all_by_file_reference_id(
    org_id: str, file_reference_id: str, with_commit: bool = True
) -> None:
    session.query(FileExtraction).filter(
        FileExtraction.organization_id == org_id,
        FileExtraction.file_reference_id == file_reference_id,
    ).delete()
    general.flush_or_commit(with_commit)
