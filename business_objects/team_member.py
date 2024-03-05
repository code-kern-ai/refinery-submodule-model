from datetime import datetime
from typing import Optional
from . import general, team, user
from ..session import session
from ..models import TeamMember


def get(team_id: str, id: str):
    return (
        session.query(TeamMember)
        .filter(TeamMember.team_id == team_id, TeamMember.id == id)
        .first()
    )


def get_by_team_and_user(team_id: str, user_id: str) -> TeamMember:
    return (
        session.query(TeamMember)
        .filter(TeamMember.team_id == team_id, TeamMember.user_id == user_id)
        .first()
    )


def get_all_by_team(team_id: str) -> list:
    return session.query(TeamMember).filter(TeamMember.team_id == team_id).all()


def get_all_by_team_count(team_id: str) -> int:
    return session.query(TeamMember).filter(TeamMember.team_id == team_id).count()


def create(
    team_id: str,
    user_id: str,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> TeamMember:
    already_exist = get_by_team_and_user(team_id=team_id, user_id=user_id)
    if already_exist:
        return already_exist

    team_item = team.get(team_id)
    user_item = user.get(user_id)
    if not team_item or not user_item:
        raise Exception("Team or user not found")
    if team_item.organization_id != user_item.organization_id:
        raise Exception("User not in the same organization as the team")

    team_member = TeamMember(
        team_id=team_id,
        user_id=user_id,
        created_by=created_by,
        created_at=created_at,
    )
    general.add(team_member, with_commit)
    return team_member


def delete_by_team_and_user_id(
    team_id: str, user_id: str, with_commit: bool = False
) -> None:
    team_member = get_by_team_and_user(team_id, user_id)
    if team_member:
        general.delete(team_member, with_commit)


def delete_by_user_id(user_id: str, with_commit: bool = False) -> None:
    team_memberships = (
        session.query(TeamMember).filter(TeamMember.user_id == user_id).all()
    )
    for membership in team_memberships:
        general.delete(membership, with_commit=False)
    general.flush_or_commit(with_commit)
