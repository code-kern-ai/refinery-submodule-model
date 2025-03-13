import datetime
from submodules.model.enums import TokenLimit
from submodules.model.session import session
from submodules.model.business_objects import general
from submodules.model.models import (
    PersonalAccessTokenActivityLogEtl,
)

FILE_UPLOAD_INTERVAL = 3600  # 1 hour in seconds
FILE_UPLOAD_LIMIT = 50  # per interval

TOKEN_LIMIT_BREACH_QUERY = """SELECT SUM(quantity)
FROM cognition.personal_access_token_activity_log_etl patale
WHERE patale.organization_id = '{org_id}'
    AND patale.action = '{upload_action}'
    AND patale.created_at > (SELECT NOW() - INTERVAL '{file_upload_interval} seconds')
"""


def get_remaining_upload_limit(
    org_id: str, file_upload_limit: int, file_upload_interval: int
) -> int:
    query = TOKEN_LIMIT_BREACH_QUERY.format(
        org_id=org_id,
        upload_action=TokenLimit.FILE_UPLOAD_LIMIT.value,
        file_upload_interval=file_upload_interval,
    )
    files_uploaded_no = general.execute_first(query)
    if not files_uploaded_no[0]:
        return file_upload_limit
    return file_upload_limit - files_uploaded_no[0]


def is_limit_breached(
    org_id: str, file_upload_limit: int, file_upload_interval: int
) -> bool:
    remaining_upload_limit = get_remaining_upload_limit(
        org_id, file_upload_limit, file_upload_interval
    )
    return remaining_upload_limit <= 0


def get_retry_after(
    org_id: str, file_upload_limit: int = None, file_upload_interval: int = None
) -> int:
    file_upload_limit = int(file_upload_limit or FILE_UPLOAD_LIMIT)
    file_upload_interval = int(file_upload_interval or FILE_UPLOAD_INTERVAL)
    if not is_limit_breached(org_id, file_upload_limit, file_upload_interval):
        return False

    latest_activity = (
        session.query(PersonalAccessTokenActivityLogEtl)
        .filter(
            PersonalAccessTokenActivityLogEtl.organization_id == org_id,
            PersonalAccessTokenActivityLogEtl.action
            == TokenLimit.FILE_UPLOAD_LIMIT.value,
        )
        .order_by(PersonalAccessTokenActivityLogEtl.created_at.desc())
        .first()
    )
    time_since_latest = (
        datetime.datetime.now(datetime.timezone.utc) - latest_activity.created_at
    )
    retry_after = file_upload_interval - time_since_latest.seconds
    return retry_after


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
    org_id: str = None,
    delete_after_days: int = 90,
    with_commit: bool = False,
) -> None:
    if org_id:
        session.query(PersonalAccessTokenActivityLogEtl).filter(
            PersonalAccessTokenActivityLogEtl.organization_id == org_id,
        ).delete()
        general.flush_or_commit(with_commit)
        return

    if delete_after_days:
        session.query(PersonalAccessTokenActivityLogEtl).filter(
            PersonalAccessTokenActivityLogEtl.created_at
            < datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=delete_after_days),
        ).delete()
        general.flush_or_commit(with_commit)
        return
