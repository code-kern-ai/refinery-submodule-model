from datetime import datetime
from typing import List, Optional
from . import general
from ..session import session
from ..models import CognitionDataroom, CognitionDataroomFile, CognitionDataroomCommit, CognitionDataroomCommitFile


def get(dataroom_id: str) -> CognitionDataroom:
    return session.query(CognitionDataroom).filter(CognitionDataroom.id == dataroom_id).first()



def get_all_by_project(project_id: str) -> List[CognitionDataroom]:
    return (
        session.query(CognitionDataroom)
        .filter(CognitionDataroom.project_id == project_id)
        .order_by(CognitionDataroom.name.asc())
        .all()
    )


def create_dataroom(
    project_id: str,
    name: str,
    description: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> CognitionDataroom:
    dataroom = CognitionDataroom(
        project_id=project_id,
        name=name,
        description=description,
        created_by=created_by,
        created_at=created_at,
    )
    general.add(dataroom, with_commit)
    return dataroom


def update_dataroom(
    dataroom_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = False,
) -> CognitionDataroom:
    dataroom = get(dataroom_id)

    if name is not None:
        dataroom.name = name
    if description is not None:
        dataroom.description = description
    general.flush_or_commit(with_commit)

    return dataroom


def delete(dataroom_id: str, with_commit: bool = False) -> None:
    dataroom = get(dataroom_id)
    if dataroom:
        general.delete(dataroom, with_commit)
