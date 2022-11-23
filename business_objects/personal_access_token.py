from ..session import session
from submodules.model.business_objects import general
from submodules.model.models import PersonalAcessToken
from datetime import date


def get(project_id: str, user_id: str, name: str):
    return (
        session.query(PersonalAcessToken)
        .filter(
            PersonalAcessToken.project_id == project_id,
            PersonalAcessToken.user_id == user_id,
            PersonalAcessToken.name == name,
        )
        .first()
    )


def get_all(project_id: str, user_id: str):
    return (
        session.query(PersonalAcessToken)
        .filter(
            PersonalAcessToken.project_id == project_id,
            PersonalAcessToken.user_id == user_id,
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
    personal_access_token = PersonalAcessToken(
        project_id=project_id,
        user_id=user_id,
        name=name,
        scope=scope,
        token=token,
        expires_at=expires_at,
    )
    general.add(personal_access_token, with_commit)
