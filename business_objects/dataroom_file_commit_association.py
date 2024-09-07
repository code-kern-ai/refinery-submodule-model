from datetime import datetime
from typing import List, Optional
from . import general
from ..session import session
from ..models import CognitionDataroomCommitFile


def get(dataroom_commit_file_id: str) -> CognitionDataroomCommitFile:
    return session.query(CognitionDataroomCommitFile).filter(CognitionDataroomCommitFile.id == dataroom_commit_file_id).first()


def get_all_by_commit(commit_id: str) -> List[CognitionDataroomCommitFile]:
    return (
        session.query(CognitionDataroomCommitFile)
        .filter(CognitionDataroomCommitFile.commit_id == commit_id)
        .order_by(CognitionDataroomCommitFile.timestamp.desc())
        .all()
    )

def get_all_by_file(file_id: str) -> List[CognitionDataroomCommitFile]:
    return (
        session.query(CognitionDataroomCommitFile)
        .filter(CognitionDataroomCommitFile.file_id == file_id)
        .order_by(CognitionDataroomCommitFile.timestamp.desc())
        .all()
    )


def create_dataroom_commit_file(
    commit_id: str,
    file_id: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> CognitionDataroomCommitFile:
    dataroom_commit_file = CognitionDataroomCommitFile(
        commit_id=commit_id,
        file_id=file_id,
        created_by=created_by,
        created_at=created_at,
    )
    general.add(dataroom_commit_file, with_commit)
    return dataroom_commit_file



def delete(dataroom_commit_file_id: str, with_commit: bool = False) -> None:
    dataroom_commit_file = get(dataroom_commit_file_id)
    if dataroom_commit_file:
        general.delete(dataroom_commit_file, with_commit)

