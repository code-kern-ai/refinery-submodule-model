import datetime
from typing import List
from submodules.model.enums import TokenLimit
from submodules.model.session import session
from submodules.model.business_objects import general
from submodules.model.models import (
    PersonalAccessTokenActivityLogEtl,
)

DEFAULT_FILE_UPLOAD_LIMIT = 50  # per hour


def get_limit_breach(
    token_scope_id: str, file_upload_limit: str = None
) -> PersonalAccessTokenActivityLogEtl:
    if file_upload_limit:
        return (
            session.query(PersonalAccessTokenActivityLogEtl)
            .filter(
                PersonalAccessTokenActivityLogEtl.token_scope_id == token_scope_id,
                PersonalAccessTokenActivityLogEtl.action
                == TokenLimit.FILE_UPLOAD.value,
            )
            .order_by(PersonalAccessTokenActivityLogEtl.created_at.desc())
            .limit(file_upload_limit)
            .all()
        )
    return (
        session.query(PersonalAccessTokenActivityLogEtl)
        .filter(
            PersonalAccessTokenActivityLogEtl.token_scope_id == token_scope_id,
        )
        .first()
    )


def create(
    org_id: str,
    action: str,
    quantity: str,
    endpoint: str,
    organization_id: datetime,
    token_scope_id: str,
    with_commit: bool = False,
) -> PersonalAccessTokenActivityLogEtl:
    pat_activity = PersonalAccessTokenActivityLogEtl(
        org_id=org_id,
        action=action,
        quantity=quantity,
        endpoint=endpoint,
        organization_id=organization_id,
        token_scope_id=token_scope_id,
    )
    general.add(pat_activity, with_commit)
    return pat_activity


def delete_many(
    token_activity_ids: List[str] = None,
    delete_after_days: int = 90,
    with_commit: bool = False,
) -> None:
    if token_activity_ids:
        session.query(PersonalAccessTokenActivityLogEtl).filter(
            PersonalAccessTokenActivityLogEtl.id.in_(token_activity_ids),
        ).delete()
        general.flush_or_commit(with_commit)
        return

    if delete_after_days:
        session.query(PersonalAccessTokenActivityLogEtl).filter(
            PersonalAccessTokenActivityLogEtl.created_at
            < datetime.datetime.now() - datetime.timedelta(days=delete_after_days),
        ).delete()
        general.flush_or_commit(with_commit)
        return
