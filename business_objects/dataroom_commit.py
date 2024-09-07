from datetime import datetime
from typing import List, Optional
from . import general
from ..session import session
from ..models import CognitionDataroomCommit


def get(dataroom_commit_id: str) -> CognitionDataroomCommit:
    return session.query(CognitionDataroomCommit).filter(CognitionDataroomCommit.id == dataroom_commit_id).first()



def get_all_by_dataroom(dataroom_id: str) -> List[CognitionDataroomCommit]:
    return (
        session.query(CognitionDataroomCommit)
        .filter(CognitionDataroomCommit.dataroom_id == dataroom_id)
        .order_by(CognitionDataroomCommit.timestamp.desc())
        .all()
    )


def create_dataroom_commit(
    dataroom_id: str,
    message: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> CognitionDataroomCommit:
    dataroom_commit = CognitionDataroomCommit(
        dataroom_id=dataroom_id,
        message=message,
        created_by=created_by,
        created_at=created_at,
    )
    general.add(dataroom_commit, with_commit)
    return dataroom_commit



def delete(dataroom_commit_id: str, with_commit: bool = False) -> None:
    dataroom_commit = get(dataroom_commit_id)
    if dataroom_commit:
        general.delete(dataroom_commit, with_commit)

