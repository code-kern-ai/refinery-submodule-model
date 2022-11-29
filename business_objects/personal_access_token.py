import datetime
from typing import List
from ..session import session
from submodules.model.business_objects import general
from submodules.model.models import PersonalAccessToken
from datetime import date


def get(project_id: str, user_id: str, name: str) -> PersonalAccessToken:
    return (
        session.query(PersonalAccessToken)
        .filter(
            PersonalAccessToken.project_id == project_id,
            PersonalAccessToken.user_id == user_id,
            PersonalAccessToken.name == name,
        )
        .first()
    )


def get_all(project_id: str) -> List[PersonalAccessToken]:
    return (
        session.query(PersonalAccessToken)
        .filter(PersonalAccessToken.project_id == project_id)
        .all()
    )


def get_by_token(project_id: str, token: str) -> PersonalAccessToken:
    return (
        session.query(PersonalAccessToken)
        .filter(
            PersonalAccessToken.project_id == project_id,
            PersonalAccessToken.token == token,
        )
        .first()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    scope: str,
    expires_at: datetime,
    token: str,
    with_commit: bool = False,
) -> None:
    personal_access_token = PersonalAccessToken(
        project_id=project_id,
        user_id=user_id,
        name=name,
        scope=scope,
        token=token,
        expires_at=expires_at,
    )
    general.add(personal_access_token, with_commit)


def delete(
    project_id: str,
    token_id: str,
    with_commit: bool = False,
) -> None:
    session.query(PersonalAccessToken).filter(
        PersonalAccessToken.project_id == project_id,
        PersonalAccessToken.id == token_id,
    ).delete()
    general.flush_or_commit(with_commit)


def update_last_used(project_id: str, token_id: str, with_commit: bool = False) -> None:
    token_item = (
        session.query(PersonalAccessToken)
        .filter(
            PersonalAccessToken.project_id == project_id,
            PersonalAccessToken.id == token_id,
        )
        .first()
    )

    token_item.last_used = datetime.datetime.now()
    general.flush_or_commit(with_commit)
