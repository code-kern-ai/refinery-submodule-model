import datetime
from typing import List
from ..session import session
from submodules.model.business_objects import general
from submodules.model.models import CognitionPersonalAccessTokenETL


def get_by_user_and_name(
    dataset_id: str,
    created_by: str,
    name: str,
) -> CognitionPersonalAccessTokenETL:
    return (
        session.query(CognitionPersonalAccessTokenETL)
        .filter(
            CognitionPersonalAccessTokenETL.dataset_id == dataset_id,
            CognitionPersonalAccessTokenETL.name == name,
            CognitionPersonalAccessTokenETL.created_by == created_by,
        )
        .first()
    )


def get_all(dataset_id: str) -> List[CognitionPersonalAccessTokenETL]:
    return (
        session.query(CognitionPersonalAccessTokenETL)
        .filter(CognitionPersonalAccessTokenETL.dataset_id == dataset_id)
        .all()
    )


def get_by_token(dataset_id: str, token: str) -> CognitionPersonalAccessTokenETL:
    return (
        session.query(CognitionPersonalAccessTokenETL)
        .filter(
            CognitionPersonalAccessTokenETL.dataset_id == dataset_id,
            CognitionPersonalAccessTokenETL.token == token,
        )
        .first()
    )


def create(
    dataset_id: str,
    created_by: str,
    name: str,
    scope: str,
    expires_at: datetime,
    token: str,
    with_commit: bool = False,
) -> CognitionPersonalAccessTokenETL:
    personal_access_token = CognitionPersonalAccessTokenETL(
        dataset_id=dataset_id,
        name=name,
        scope=scope,
        token=token,
        expires_at=expires_at,
        created_by=created_by,
    )
    general.add(personal_access_token, with_commit)
    return personal_access_token


def delete(
    dataset_id: str,
    token_id: str,
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessTokenETL).filter(
        CognitionPersonalAccessTokenETL.dataset_id == dataset_id,
        CognitionPersonalAccessTokenETL.id == token_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_token_by_ids(
    dataset_id: str,
    token_ids: List[str],
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessTokenETL).filter(
        CognitionPersonalAccessTokenETL.dataset_id == dataset_id,
        CognitionPersonalAccessTokenETL.id.in_(token_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def update_last_used(
    dataset_id: str,
    token_id: str,
    with_commit: bool = False,
) -> None:
    token_item = (
        session.query(CognitionPersonalAccessTokenETL)
        .filter(
            CognitionPersonalAccessTokenETL.dataset_id == dataset_id,
            CognitionPersonalAccessTokenETL.id == token_id,
        )
        .first()
    )

    token_item.last_used = datetime.datetime.now()
    general.flush_or_commit(with_commit)
