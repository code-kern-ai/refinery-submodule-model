from ..business_objects import general
from ..session import session
from ..models import FileExtraction


def get(org_id: str, file_reference_id: str, ext_method: str) -> FileExtraction:
    return (
        session.query(FileExtraction)
        .filter(
            FileExtraction.organization_id == org_id,
            FileExtraction.file_reference_id == file_reference_id,
            FileExtraction.extraction_method == ext_method,
        )
        .first()
    )


def create(
    org_id: str,
    file_reference_id: str,
    created_by: str,
    minio_path: str,
    with_commit: bool = True,
) -> FileExtraction:

    file_extraction = FileExtraction(
        organization_id=org_id,
        file_reference_id=file_reference_id,
        minio_path=minio_path,
        bucket=org_id,
        created_by=created_by,
    )

    general.add(file_extraction, with_commit)

    return file_extraction
