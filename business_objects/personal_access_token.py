import datetime
from typing import List, Optional, Union
from ..session import session
from submodules.model.business_objects import general
from submodules.model.models import PersonalAccessToken, CognitionPersonalAccessToken


def __get_token_type(
    in_cognition_scope,
) -> Union[PersonalAccessToken, CognitionPersonalAccessToken]:
    if in_cognition_scope:
        return CognitionPersonalAccessToken
    else:
        return PersonalAccessToken


def get_by_user_and_name(
    project_id: str, user_id: str, name: str, in_cognition_scope: Optional[bool] = False
) -> Union[PersonalAccessToken, CognitionPersonalAccessToken]:
    token_table = __get_token_type(in_cognition_scope)
    return (
        session.query(token_table)
        .filter(
            token_table.project_id == project_id,
            token_table.user_id == user_id,
            token_table.name == name,
        )
        .first()
    )


def get_all(
    project_id: str, in_cognition_scope: Optional[bool] = False
) -> List[Union[PersonalAccessToken, CognitionPersonalAccessToken]]:
    token_table = __get_token_type(in_cognition_scope)

    return session.query(token_table).filter(token_table.project_id == project_id).all()


def get_by_token(
    project_id: str, token: str, in_cognition_scope: Optional[bool] = False
) -> Union[PersonalAccessToken, CognitionPersonalAccessToken]:
    token_table = __get_token_type(in_cognition_scope)
    return (
        session.query(token_table)
        .filter(
            token_table.project_id == project_id,
            token_table.token == token,
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
    in_cognition_scope: Optional[bool] = False,
    with_commit: bool = False,
) -> None:
    token_table = __get_token_type(in_cognition_scope)
    personal_access_token = token_table(
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
    in_cognition_scope: Optional[bool] = False,
    with_commit: bool = False,
) -> None:
    token_table = __get_token_type(in_cognition_scope)
    session.query(token_table).filter(
        token_table.project_id == project_id,
        token_table.id == token_id,
    ).delete()
    general.flush_or_commit(with_commit)


def update_last_used(
    project_id: str,
    token_id: str,
    with_commit: bool = False,
    in_cognition_scope: Optional[bool] = False,
) -> None:
    token_table = __get_token_type(in_cognition_scope)
    token_item = (
        session.query(token_table)
        .filter(
            token_table.project_id == project_id,
            token_table.id == token_id,
        )
        .first()
    )

    token_item.last_used = datetime.datetime.now()
    general.flush_or_commit(with_commit)
