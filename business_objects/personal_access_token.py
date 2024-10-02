import datetime
from typing import List
from ..session import session
from submodules.model.business_objects import general
from submodules.model.models import CognitionPersonalAccessToken


def get_by_user_and_name(
    project_id: str,
    created_by: str,
    name: str,
) -> CognitionPersonalAccessToken:
    return (
        session.query(CognitionPersonalAccessToken)
        .filter(
            CognitionPersonalAccessToken.project_id == project_id,
            CognitionPersonalAccessToken.name == name,
            CognitionPersonalAccessToken.created_by == created_by,
        )
        .first()
    )


def get_all(project_id: str) -> List[CognitionPersonalAccessToken]:
    return (
        session.query(CognitionPersonalAccessToken)
        .filter(CognitionPersonalAccessToken.project_id == project_id)
        .all()
    )


def get_by_token(project_id: str, token: str) -> CognitionPersonalAccessToken:
    return (
        session.query(CognitionPersonalAccessToken)
        .filter(
            CognitionPersonalAccessToken.project_id == project_id,
            CognitionPersonalAccessToken.token == token,
        )
        .first()
    )


def create(
    project_id: str,
    created_by: str,
    name: str,
    scope: str,
    expires_at: datetime,
    token: str,
    with_commit: bool = False,
) -> CognitionPersonalAccessToken:
    personal_access_token = CognitionPersonalAccessToken(
        project_id=project_id,
        name=name,
        scope=scope,
        token=token,
        expires_at=expires_at,
        created_by=created_by,
    )
    general.add(personal_access_token, with_commit)
    return personal_access_token


def delete(
    project_id: str,
    token_id: str,
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessToken).filter(
        CognitionPersonalAccessToken.project_id == project_id,
        CognitionPersonalAccessToken.id == token_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_token_by_ids(
    project_id: str,
    token_ids: List[str],
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessToken).filter(
        CognitionPersonalAccessToken.project_id == project_id,
        CognitionPersonalAccessToken.id.in_(token_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def update_last_used(
    project_id: str,
    token_id: str,
    with_commit: bool = False,
) -> None:
    token_item = (
        session.query(CognitionPersonalAccessToken)
        .filter(
            CognitionPersonalAccessToken.project_id == project_id,
            CognitionPersonalAccessToken.id == token_id,
        )
        .first()
    )

    token_item.last_used = datetime.datetime.now()
    general.flush_or_commit(with_commit)
