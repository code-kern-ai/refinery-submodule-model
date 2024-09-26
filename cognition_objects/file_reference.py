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
    original_file_name: str,
    content_type: str,
    minio_path: str,
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
    )

    general.add(file_reference, with_commit)

    return file_reference
