from ..session import session
from submodules.model.business_objects import general
from submodules.model.models import PersonalAccessToken
from datetime import date


def get(project_id: str, user_id: str, name: str):
    return (
        session.query(PersonalAccessToken)
        .filter(
            PersonalAccessToken.project_id == project_id,
            PersonalAccessToken.user_id == user_id,
            PersonalAccessToken.name == name,
        )
        .first()
    )


def get_all(project_id: str, user_id: str):
    return (
        session.query(PersonalAccessToken)
        .filter(
            PersonalAccessToken.project_id == project_id,
            PersonalAccessToken.user_id == user_id,
        )
        .all()
    )


def get_by_token(project_id: str, token: str):
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
    expires_at: date,
    token: str,
    with_commit: bool = False,
):
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
    user_id: str,
    name: str,
    with_commit: bool = False,
):
    session.query(PersonalAccessToken).filter(
        PersonalAccessToken.project_id == project_id,
        PersonalAccessToken.user_id == user_id,
        PersonalAccessToken.name == name,
    ).delete()
    general.flush_or_commit(with_commit)
