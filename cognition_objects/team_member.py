from typing import List, Optional, Dict
from datetime import datetime
from ..business_objects import general
from ..session import session
from ..models import (
    CognitionTeamMember,
)


def get(team_member_id: str) -> CognitionTeamMember:
    return (
        session.query(CognitionTeamMember)
        .filter(
            CognitionTeamMember.id == team_member_id,
        )
        .first()
    )


def get_all_by_team_id(team_id: str) -> List[CognitionTeamMember]:
    return (
        session.query(CognitionTeamMember)
        .filter(CognitionTeamMember.team_id == team_id)
        .order_by(CognitionTeamMember.created_at.asc())
        .all()
    )


def get_all_by_user_id(user_id: str) -> List[CognitionTeamMember]:
    return (
        session.query(CognitionTeamMember)
        .filter(CognitionTeamMember.user_id == user_id)
        .order_by(CognitionTeamMember.created_at.asc())
        .all()
    )


def create(
    team_id: str,
    user_id: str,
    created_by: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionTeamMember:
    team_member: CognitionTeamMember = CognitionTeamMember(
        created_by=user_id,
        team_id=team_id,
        user_id=user_id,
        created_at=created_at,
    )
    general.add(team_member, with_commit)
    return team_member


def delete(team_member_id: str, with_commit: bool = True) -> None:
    session.query(CognitionTeamMember).filter(
        CognitionTeamMember.id == team_member_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_by_team_and_user_id(
    team_id: str, user_id: str, with_commit: bool = True
) -> None:
    session.query(CognitionTeamMember).filter(
        CognitionTeamMember.team_id == team_id,
        CognitionTeamMember.user_id == user_id,
    ).delete()
    general.flush_or_commit(with_commit)
