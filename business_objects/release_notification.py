from typing import List
from ..models import ReleaseNotification
from . import general
from ..session import session
from datetime import datetime


def get(release_notification_id: str) -> ReleaseNotification:
    return (
        session.query(ReleaseNotification)
        .filter(ReleaseNotification.id == release_notification_id)
        .first()
    )


def get_all() -> List[ReleaseNotification]:
    return (
        session.query(ReleaseNotification)
        .order_by(ReleaseNotification.created_at.desc())
        .all()
    )


def create(
    link: str,
    config: str,
    created_by: str,
    with_commit: bool = False,
) -> ReleaseNotification:
    release_notification = ReleaseNotification(
        link=link, config=config, created_by=created_by
    )
    general.add(release_notification, with_commit)
    return release_notification


def update(
    notification_id: str,
    link: str,
    config: str,
    with_commit: bool = False,
) -> ReleaseNotification:
    release_notification = get(notification_id)

    if release_notification is None:
        return

    if link is not None:
        release_notification.link = link
    if config is not None:
        release_notification.config = config

    general.flush_or_commit(with_commit)
    return release_notification


def delete(notification_id: str, with_commit: bool = False):
    release_notification = get(notification_id)
    general.delete(release_notification, with_commit)
    general.flush_or_commit(with_commit)
