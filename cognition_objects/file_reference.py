from ..business_objects import general
from ..session import session
from ..models import FileReference
from typing import Dict, Any, List


def get(org_id: str, hash: str, file_size_bytes) -> FileReference:
    return (
        session.query(FileReference)
        .filter(
            FileReference.organization_id == org_id,
            FileReference.hash == hash,
            FileReference.file_size_bytes == file_size_bytes,
        )
        .first()
    )


def get_by_id(org_id: str, file_reference_id: str) -> FileReference:
    return (
        session.query(FileReference)
        .filter(
            FileReference.organization_id == org_id,
            FileReference.id == file_reference_id,
        )
        .first()
    )


def get_all(org_id: str, page: int, limit: int) -> List[FileReference]:
    return (
        session.query(FileReference)
        .filter(
            FileReference.organization_id == org_id,
        )
        .limit(limit)
        .offset(max(0, (page - 1) * limit))
        .all()
    )


def create(
    org_id: str,
    hash: str,
    file_size_bytes: int,
    created_by: str,
    original_file_name: str,
    content_type: str,
    minio_path: str,
    meta_data: Dict[str, Any],
    with_commit: bool = True,
) -> FileReference:

    file_reference = FileReference(
        organization_id=org_id,
        hash=hash,
        minio_path=minio_path,
        bucket=org_id,
        file_size_bytes=file_size_bytes,
        created_by=created_by,
        original_file_name=original_file_name,
        content_type=content_type,
        meta_data=meta_data,
    )

    general.add(file_reference, with_commit)

    return file_reference


def delete(org_id: str, file_reference_id: str, with_commit: bool = True) -> None:
    session.query(FileReference).filter(
        FileReference.organization_id == org_id,
        FileReference.id == file_reference_id,
    ).delete()
    general.flush_or_commit(with_commit)
