import datetime
from typing import List
from ..session import session
from submodules.model.enums import TokenSubject
from submodules.model.business_objects import general
from submodules.model.models import (
    CognitionPersonalAccessTokenEtl,
    CognitionPersonalAccessTokenScopeEtl,
)


def get_by_user_and_name(
    created_by: str,
    name: str,
) -> CognitionPersonalAccessTokenEtl:
    return (
        session.query(CognitionPersonalAccessTokenEtl)
        .filter(
            CognitionPersonalAccessTokenEtl.name == name,
            CognitionPersonalAccessTokenEtl.created_by == created_by,
        )
        .first()
    )


def get_all(user_id: str) -> List[CognitionPersonalAccessTokenEtl]:
    return (
        session.query(CognitionPersonalAccessTokenEtl)
        .filter(CognitionPersonalAccessTokenEtl.created_by == user_id)
        .all()
    )


def get_by_token(token: str) -> CognitionPersonalAccessTokenEtl:
    return (
        session.query(CognitionPersonalAccessTokenEtl)
        .filter(
            CognitionPersonalAccessTokenEtl.token == token,
        )
        .first()
    )


def create(
    subject_id: str,
    created_by: str,
    name: str,
    scope: str,
    expires_at: datetime,
    token: str,
    subject: str = TokenSubject.PROJECT.value,
    with_commit: bool = False,
) -> CognitionPersonalAccessTokenEtl:
    pat = CognitionPersonalAccessTokenEtl(
        name=name,
        token=token,
        expires_at=expires_at,
        created_by=created_by,
    )
    pat_scope = CognitionPersonalAccessTokenScopeEtl(
        scope=scope, subject=subject, subject_id=subject_id, token_id=pat.id
    )
    general.add(pat)
    general.add(pat_scope, with_commit)

    return pat


def delete(
    token_id: str,
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessTokenEtl).filter(
        CognitionPersonalAccessTokenEtl.id == token_id
    ).delete()
    general.flush_or_commit(with_commit)


def delete_token_by_ids(
    token_ids: List[str],
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessTokenEtl).filter(
        CognitionPersonalAccessTokenEtl.id.in_(token_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def update_last_used(
    token_id: str,
    with_commit: bool = False,
) -> None:
    token_item = (
        session.query(CognitionPersonalAccessTokenEtl)
        .filter(
            CognitionPersonalAccessTokenEtl.id == token_id,
        )
        .first()
    )

    token_item.last_used = datetime.datetime.now()
    general.flush_or_commit(with_commit)
