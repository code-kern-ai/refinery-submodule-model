from typing import List, Optional, Dict
from datetime import datetime
from ..business_objects import general
from ..session import session
from ..models import (
    CognitionTeamProjectAccess,
)


def get(team_project_access_id: str) -> CognitionTeamProjectAccess:
    return (
        session.query(CognitionTeamProjectAccess)
        .filter(
            CognitionTeamProjectAccess.id == team_project_access_id,
        )
        .first()
    )


def get_all_by_team_id(team_id: str) -> List[CognitionTeamProjectAccess]:
    return (
        session.query(CognitionTeamProjectAccess)
        .filter(CognitionTeamProjectAccess.team_id == team_id)
        .order_by(CognitionTeamProjectAccess.created_at.asc())
        .all()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionTeamProjectAccess]:
    return (
        session.query(CognitionTeamProjectAccess)
        .filter(CognitionTeamProjectAccess.project_id == project_id)
        .order_by(CognitionTeamProjectAccess.created_at.asc())
        .all()
    )


def create(
    user_id: str,
    team_id: str,
    project_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionTeamProjectAccess:
    team_project_access: CognitionTeamProjectAccess = CognitionTeamProjectAccess(
        created_by=user_id,
        team_id=team_id,
        project_id=project_id,
        created_at=created_at,
    )
    general.add(team_project_access, with_commit)
    return team_project_access


def delete(team_project_access_id: str, with_commit: bool = True) -> None:
    session.query(CognitionTeamProjectAccess).filter(
        CognitionTeamProjectAccess.id == team_project_access_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_by_team_and_project_id(
    team_id: str, project_id: str, with_commit: bool = True
) -> None:
    session.query(CognitionTeamProjectAccess).filter(
        CognitionTeamProjectAccess.team_id == team_id,
        CognitionTeamProjectAccess.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
