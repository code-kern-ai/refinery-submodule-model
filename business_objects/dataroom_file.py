from datetime import datetime
from typing import List, Optional
from . import general
from ..session import session
from ..models import CognitionDataroomFile


def get(dataroom_file_id: str) -> CognitionDataroomFile:
    return session.query(CognitionDataroomFile).filter(CognitionDataroomFile.id == dataroom_file_id).first()



def get_all_by_dataroom(dataroom_id: str) -> List[CognitionDataroomFile]:
    return (
        session.query(CognitionDataroomFile)
        .filter(CognitionDataroomFile.dataroom_id == dataroom_id)
        .order_by(CognitionDataroomFile.name.asc())
        .all()
    )


def create_dataroom_file(
    dataroom_id: str,
    name: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> CognitionDataroomFile:
    dataroom_file = CognitionDataroomFile(
        dataroom_id=dataroom_id,
        name=name,
        created_by=created_by,
        created_at=created_at,
    )
    general.add(dataroom_file, with_commit)
    return dataroom_file



def delete(dataroom_file_id: str, with_commit: bool = False) -> None:
    dataroom_file = get(dataroom_file_id)
    if dataroom_file:
        general.delete(dataroom_file, with_commit)

