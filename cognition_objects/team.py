from typing import List, Optional, Dict
from datetime import datetime
from ..business_objects import general
from ..session import session
from ..models import (
    CognitionTeam,
)
from ..util import prevent_sql_injection
from sqlalchemy import or_


def get(team_id: str) -> CognitionTeam:
    return (
        session.query(CognitionTeam)
        .filter(
            CognitionTeam.id == team_id,
        )
        .first()
    )


def get_all_by_org_id(org_id: str) -> List[CognitionTeam]:
    # Note, atm this doesn't mean all but all on org level
    return (
        session.query(CognitionTeam)
        .filter(CognitionTeam.organization_id == org_id)
        .order_by(CognitionTeam.created_at.asc())
        .all()
    )


def create(
    organization_id: str,
    user_id: str,
    name: str,
    description: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionTeam:
    team: CognitionTeam = CognitionTeam(
        created_by=user_id,
        organization_id=organization_id,
        name=name,
        description=description,
        created_at=created_at,
    )
    general.add(team, with_commit)
    return team


def update(
    team_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionTeam:
    team: CognitionTeam = get(team_id)

    if name is not None:
        team.name = name
    if description is not None:
        team.description = description
    general.flush_or_commit(with_commit)
    return team


def delete(team_id: str, with_commit: bool = True) -> None:
    session.query(CognitionTeam).filter(
        CognitionTeam.id == team_id,
    ).delete()
    general.flush_or_commit(with_commit)
