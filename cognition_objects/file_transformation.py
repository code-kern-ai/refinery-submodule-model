from ..business_objects import general
from ..session import session
from ..models import FileTransformation


def get(
    org_id: str, file_extraction_id: str, transformation_key: str
) -> FileTransformation:
    return (
        session.query(FileTransformation)
        .filter(
            FileTransformation.organization_id == org_id,
            FileTransformation.file_extraction_id == file_extraction_id,
            FileTransformation.transformation_key == transformation_key,
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
