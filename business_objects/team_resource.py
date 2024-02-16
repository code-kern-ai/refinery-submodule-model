from datetime import datetime
from typing import Optional, List
from . import general, team
from ..cognition_objects import project as cognition_project
from .. import enums
from ..session import session
from ..models import TeamResource


def get(team_id: str, id: str) -> TeamResource:
    return (
        session.query(TeamResource)
        .filter(TeamResource.team_id == team_id, TeamResource.id == id)
        .first()
    )


def get_by_team_and_resource(
    team_id: str, resource_id: str, resource_type: enums.ResourceType
) -> TeamResource:
    return (
        session.query(TeamResource)
        .filter(
            TeamResource.team_id == team_id,
            TeamResource.resource_id == resource_id,
            TeamResource.resource_type == resource_type.value,
        )
        .first()
    )


def get_all_by_team(team_id: str, resource_type: enums.ResourceType) -> List:
    return (
        session.query(TeamResource)
        .filter(
            TeamResource.team_id == team_id,
            TeamResource.resource_type == resource_type.value,
        )
        .all()
    )


def get_all_by_resource(resource_id: str, resource_type: enums.ResourceType) -> List:
    return (
        session.query(TeamResource)
        .filter(
            TeamResource.resource_id == resource_id,
            TeamResource.resource_type == resource_type.value,
        )
        .all()
    )


def create(
    team_id: str,
    resource_id: str,
    resource_type: enums.ResourceType,
    created_by: str,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> TeamResource:
    already_exist = get_by_team_and_resource(team_id, resource_id, resource_type)
    if already_exist:
        return already_exist

    team_item = team.get(team_id)
    resource_item = __get_resource(resource_id, resource_type)
    if not team_item or not resource_item:
        return None
    if team_item.organization_id != resource_item.organization_id:
        return None

    team_resource = TeamResource(
        team_id=team_id,
        resource_id=resource_id,
        resource_type=resource_type.value,
        created_by=created_by,
        created_at=created_at,
    )
    general.add(team_resource, with_commit)
    return team_resource


def __get_resource(resource_id: str, resource_type: enums.ResourceType):
    if resource_type == enums.ResourceType.COGNITION_PROJECT:
        return cognition_project.get(resource_id)
    else:
        return None


def delete_by_team_and_resource(
    team_id: str,
    resource_id: str,
    resource_type: enums.ResourceType,
    with_commit: bool = False,
) -> None:
    team_resource = get_by_team_and_resource(team_id, resource_id, resource_type)
    if team_resource:
        general.delete(team_resource, with_commit)


def delete_by_resource(
    resource_id: str, resource_type: enums.ResourceType, with_commit: bool = False
) -> None:
    team_resources = (
        session.query(TeamResource)
        .filter(
            TeamResource.resource_id == resource_id,
            TeamResource.resource_type == resource_type.value,
        )
        .all()
    )
    for team_resource in team_resources:
        general.delete(team_resource, with_commit=False)
    general.flush_or_commit(with_commit)
