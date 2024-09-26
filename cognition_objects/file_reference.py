from typing import List, Optional, Dict, Union, Literal
from ..business_objects import general
from ..session import session
from ..models import FileReference


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


def create(
    org_id: str,
    hash: str,
    file_size_bytes: int,
    created_by: str,
    content_type: str,
    original_file_name: str,
    with_commit: bool = True,
) -> FileReference:

    minio_path = f"files/{hash}_{file_size_bytes}"

    file_reference = FileReference(
        organization_id=org_id,
        hash=hash,
        minio_path=minio_path,
        bucket=org_id,
        file_size_bytes=file_size_bytes,
        created_by=created_by,
        content_type=content_type,
        original_file_name=original_file_name,
    )

    general.add(file_reference, with_commit)

    return file_reference
