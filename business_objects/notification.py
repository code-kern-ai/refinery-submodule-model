from _operator import or_
from datetime import datetime, timedelta
from typing import List

from . import general
from .. import Notification, NotificationState, models, enums
from ..session import session


def get_duplicated(
    project_id: str, notification_type: str, user_id: str
) -> Notification:
    return (
        session.query(models.Notification)
        .filter(
            models.Notification.type == notification_type,
            models.Notification.project_id == project_id,
            models.Notification.user_id == user_id,
            models.Notification.created_at >= (datetime.now() - timedelta(seconds=2)),
        )
        .first()
    )


def get_notifications_by_user_id(user_id: str) -> List[Notification]:
    notifications: List[Notification] = (
        session.query(Notification)
        .filter(
            Notification.user_id == user_id,
            Notification.state == NotificationState.INITIAL.value,
        )
        .all()
    )

    for notification in notifications:
        notification.state = NotificationState.NOT_INITIAL.value
    general.commit()
    return notifications


def get_filtered_notification(
    user: models.User,
    project_filter: List[str],
    level_filter: List[str],
    type_filter: List[str],
    user_filter: bool,
    limit: int,
):
    query = session.query(models.Notification)

    # user part
    if user_filter:
        query = query.filter(models.Notification.user_id == user.id)

    # target part
    if project_filter:
        query = query.filter(models.Notification.project_id.in_(project_filter))
    else:
        projects = (
            session.query(models.Project)
            .filter(models.Project.organization_id == user.organization_id)
            .all()
        )
        query = query.filter(
            or_(
                models.Notification.project_id.in_(
                    [project.id for project in projects]
                ),
                models.Notification.user_id == user.id,
            )
        )

    # level part
    if level_filter:
        for level in level_filter:
            if level not in [level.value for level in enums.Notification]:
                raise Exception(f"Level {level_filter} is not valid for notifications.")
        query = query.filter(models.Notification.level.in_(level_filter))

    # type part
    if type_filter:
        query = query.filter(models.Notification.type.in_(type_filter))

    query = query.order_by(models.Notification.created_at.desc()).limit(limit)
    return query.all()


def create(
    project_id: str,
    user_id: str,
    message: str,
    level: str,
    notification_type: str,
    with_commit: bool = False,
) -> Notification:

    notification: Notification = models.Notification(
        message=message,
        important=False,
        state=enums.NotificationState.INITIAL.value,
        level=level,
        user_id=user_id,
        project_id=project_id,
        type=notification_type,
    )
    general.add(notification, with_commit)
    return notification
