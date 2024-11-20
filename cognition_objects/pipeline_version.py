from typing import List, Optional, Dict, Any
from ..business_objects import general
from ..session import session
from ..models import CognitionPipelineVersion
from .. import enums

MAX_AUTO_SAVE_VERSIONS = 10


def get_current_version(project_id: str) -> Optional[CognitionPipelineVersion]:
    return (
        session.query(CognitionPipelineVersion)
        .filter(
            CognitionPipelineVersion.project_id == project_id,
        )
        .order_by(CognitionPipelineVersion.created_at.desc())
        .first()
    )


def get_version_by_id(project_id: str, version_id: str) -> CognitionPipelineVersion:
    return (
        session.query(CognitionPipelineVersion)
        .filter(
            CognitionPipelineVersion.project_id == project_id,
            CognitionPipelineVersion.id == version_id,
        )
        .first()
    )


def get_all_versions(project_id: str) -> List[CognitionPipelineVersion]:
    return (
        session.query(CognitionPipelineVersion)
        .filter(CognitionPipelineVersion.project_id == project_id)
        .all()
    )


def create_pipeline_version(
    project_id: str,
    user_id: str,
    version_dict: Dict[str, Any],
    with_commit: bool = True,
) -> CognitionPipelineVersion:

    current_count = (
        session.query(CognitionPipelineVersion)
        .filter(
            CognitionPipelineVersion.project_id == project_id,
            CognitionPipelineVersion.version_type
            == enums.PipelineVersionType.AUTO_SAVE.value,
        )
        .count()
    )

    if current_count >= MAX_AUTO_SAVE_VERSIONS:
        oldest = (
            session.query(CognitionPipelineVersion)
            .filter(
                CognitionPipelineVersion.project_id == project_id,
                CognitionPipelineVersion.version_type
                != enums.PipelineVersionType.PERSISTED.value,
            )
            .order_by(CognitionPipelineVersion.created_at.asc())
            .limit(current_count - MAX_AUTO_SAVE_VERSIONS + 1)
            .all()
        )
        for version in oldest:
            delete(project_id, version.id, False)

    version = CognitionPipelineVersion(
        project_id=project_id,
        # name="", # no name for auto save
        version_type=enums.PipelineVersionType.AUTO_SAVE.value,
        created_by=user_id,
        pipeline_dump=version_dict,
    )
    general.add(version, with_commit)
    return version


def persist_version(
    project_id: str, version_id: str, name: Optional[str] = None
) -> None:
    version = get_version_by_id(project_id, version_id)
    if name:
        version.name = name
    version.version_type = enums.PipelineVersionType.PERSISTED.value
    general.commit()


def delete(project_id: str, version_id: str, with_commit: bool = True) -> None:
    session.query(CognitionPipelineVersion).filter(
        CognitionPipelineVersion.id == version_id,
        CognitionPipelineVersion.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
