from ..business_objects import general
from ..session import session
from ..models import FileExtraction


def get(org_id: str, file_reference_id: str, extraction_key: str) -> FileExtraction:
    return (
        session.query(FileExtraction)
        .filter(
            FileExtraction.organization_id == org_id,
            FileExtraction.file_reference_id == file_reference_id,
            FileExtraction.extraction_key == extraction_key,
        )
        .first()
    )


def get_by_id(org_id, file_extraction_id: str) -> FileExtraction:
    return (
        session.query(FileExtraction)
        .filter(
            FileExtraction.organization_id == org_id,
            FileExtraction.id == file_extraction_id,
        )
        .first()
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
