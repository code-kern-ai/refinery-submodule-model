from typing import List
from ..models import AdminMessage
from . import general
from ..session import session
from datetime import datetime


def get(message_id: str) -> AdminMessage:
    return session.query(AdminMessage).filter(AdminMessage.id == message_id).first()


def get_all(limit: int = 100) -> List[AdminMessage]:
    return session.query(AdminMessage).filter().limit(limit).all()


def get_all_active(limit: int = 100) -> List[AdminMessage]:
    return (
        session.query(AdminMessage)
        .filter(AdminMessage.archived == False)
        .limit(limit)
        .all()
    )


def create(
    text: str,
    level: str,
    archive_date: int,
    scheduled_date: int,
    created_by: str,
    with_commit: bool = False,
) -> AdminMessage:
    message = AdminMessage(
        text=text,
        level=level,
        archive_date=archive_date,
        scheduled_date=scheduled_date,
        created_by=created_by,
    )
    general.add(message, with_commit)
    return message


def archive(
    message_id: str,
    archived_by: str,
    archive_date: datetime,
    archived_reason: str,
    with_commit: bool = False,
) -> None:
    message = get(message_id)
    if not message:
        return
    message.archived = True
    message.archived_by = archived_by
    message.archive_date = archive_date
    message.archived_reason = archived_reason
    general.flush_or_commit(with_commit)
