import datetime
from typing import List
from submodules.model.enums import TokenLimit
from submodules.model.session import session
from submodules.model.business_objects import general
from submodules.model.models import (
    PersonalAccessTokenActivityLogEtl,
)

FILE_UPLOAD_INTERVAL = 3600  # 1 hour in seconds
FILE_UPLOAD_LIMIT = 50  # per interval
FILE_UPLOAD_ACTION = TokenLimit.FILE_UPLOAD.value

TOKEN_LIMIT_BREACH_QUERY = """SELECT SUM(quantity)
FROM cognition.personal_access_token_activity_log_etl patale
WHERE patale.organization_id = '{org_id}'
    AND patale.action = '{upload_action}'
    AND patale.created_at > (SELECT NOW() - INTERVAL '{file_upload_interval} seconds')
"""


def is_limit_breached(org_id: str, file_upload_limit: int = None) -> bool:
    file_upload_limit = int(file_upload_limit or FILE_UPLOAD_LIMIT)
    query = TOKEN_LIMIT_BREACH_QUERY.format(
        org_id=org_id,
        upload_action=FILE_UPLOAD_ACTION,
        file_upload_interval=FILE_UPLOAD_INTERVAL,
    )
    files_uploaded_no = general.execute_first(query)
    if not files_uploaded_no:
        return False

    return files_uploaded_no[0] >= file_upload_limit


def get_retry_after(org_id: str, file_upload_limit: int = None) -> int:
    if not is_limit_breached(org_id, file_upload_limit):
        return False

    latest_activity = (
        session.query(PersonalAccessTokenActivityLogEtl)
        .filter(
            PersonalAccessTokenActivityLogEtl.organization_id == org_id,
            PersonalAccessTokenActivityLogEtl.action == FILE_UPLOAD_ACTION,
        )
        .order_by(PersonalAccessTokenActivityLogEtl.created_at.desc())
        .first()
    )
    retry_after = (
        FILE_UPLOAD_INTERVAL
        - (datetime.datetime.now() - latest_activity.created_at).seconds
    )
    return retry_after.seconds


def create(
    org_id: str,
    action: str,
    quantity: int,
    endpoint: str,
    token_scope_id: str,
    with_commit: bool = True,
) -> PersonalAccessTokenActivityLogEtl:
    pat_activity = PersonalAccessTokenActivityLogEtl(
        organization_id=org_id,
        action=action,
        quantity=quantity,
        endpoint=endpoint,
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


def _create_dummy_pat_activity():
    import random
    import uuid

    org_id = uuid.UUID("d1f11be8-4944-47b8-a8f3-6ddcbb27fafc")
    token_scope_id = uuid.UUID("a2acf86b-9ef1-4806-a2f6-3e1572b43780")
    for i in range(10):
        pat_activity = create(  # noqa
            org_id=org_id,
            action=TokenLimit.FILE_UPLOAD.value,
            quantity=random.randint(1, 5),
            endpoint="/cognition-api/api/v1/converters/external/parse/",
            token_scope_id=token_scope_id,
        )
