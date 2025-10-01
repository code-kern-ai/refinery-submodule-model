from typing import List
from ..models import ReleaseNotification
from . import general
from ..session import session
from datetime import datetime


def get(message_id: str) -> ReleaseNotification:
    return (
        session.query(ReleaseNotification)
        .filter(ReleaseNotification.id == message_id)
        .first()
    )


def get_all() -> List[ReleaseNotification]:
    return session.query(ReleaseNotification).filter().all()


def create(
    link: str,
    config: str,
    created_by: str,
    with_commit: bool = False,
) -> ReleaseNotification:
    message = ReleaseNotification(
        link=link, config=config, created_by=created_by, created_at=datetime.utcnow()
    )
    general.add(message, with_commit)
    return message
