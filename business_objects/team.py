from datetime import datetime
from typing import List, Optional
from . import general
from ..session import session
from ..models import Team


def get(team_id: str) -> Team:
    return session.query(Team).filter(Team.id == team_id).first()


def get_with_organization_id(organization_id: str, team_id: str) -> Team:
    return (
        session.query(Team)
        .filter(Team.organization_id == organization_id, Team.id == team_id)
        .first()
    )


def get_all(organization_id: str) -> List[Team]:
    return (
        session.query(Team)
        .filter(Team.organization_id == organization_id)
        .order_by(Team.name.asc())
        .all()
    )


def create_team(
    organization_id: str,
    name: str,
    description: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> Team:
    team = Team(
        organization_id=organization_id,
        name=name,
        description=description,
        created_by=created_by,
        created_at=created_at,
    )
    general.add(team, with_commit)
    return team


def update_team(
    team_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = False,
) -> Team:
    team = get(team_id)

    if name is not None:
        team.name = name
    if description is not None:
        team.description = description
    general.flush_or_commit(with_commit)

    return team


def delete(organization_id: str, team_id: str, with_commit: bool = False) -> None:
    team = get_with_organization_id(organization_id, team_id)
    if team:
        general.delete(team, with_commit)
