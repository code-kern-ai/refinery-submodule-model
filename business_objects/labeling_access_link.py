from datetime import datetime
from submodules.model.models import LabelingAccessLink, UserLinkConnection
from . import general
from .. import enums
from ..session import session
from typing import List, Optional


def get(link_id: str) -> LabelingAccessLink:
    return session.query(LabelingAccessLink).get(link_id)


def get_ensure_access(user_id: str, link_id: str) -> LabelingAccessLink:
    link = (
        session.query(LabelingAccessLink)
        .join(
            UserLinkConnection,
            (UserLinkConnection.user_id == user_id)
            & (UserLinkConnection.link_id == LabelingAccessLink.id)
            & (UserLinkConnection.is_locked == False),
        )
        .filter(LabelingAccessLink.id == link_id)
        .first()
    )
    if not link:
        raise Exception("No access to this link")
    return link


def get_by_all_by_project_id(project_id: str) -> List[LabelingAccessLink]:
    return (
        session.query(LabelingAccessLink)
        .filter(LabelingAccessLink.project_id == project_id)
        .all()
    )


def get_by_all_by_user_id(user_id: str) -> List[LabelingAccessLink]:
    return (
        session.query(LabelingAccessLink)
        .join(
            UserLinkConnection,
            (UserLinkConnection.user_id == user_id)
            & (UserLinkConnection.link_id == LabelingAccessLink.id)
            & (UserLinkConnection.is_locked == False),
        )
        .all()
    )


def get_by_all_by_project_user(
    project_id: str, user_id: str
) -> List[LabelingAccessLink]:
    return (
        session.query(LabelingAccessLink)
        .join(
            UserLinkConnection,
            (UserLinkConnection.user_id == user_id)
            & (UserLinkConnection.link_id == LabelingAccessLink.id)
            & (UserLinkConnection.is_locked == False),
        )
        .filter(LabelingAccessLink.project_id == project_id)
        .all()
    )


def create(
    project_id: str,
    link: str,
    link_type: enums.LinkTypes,
    created_by: str,
    data_slice_id: Optional[str] = None,
    heuristic_id: Optional[str] = None,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> LabelingAccessLink:

    link = LabelingAccessLink(
        project_id=project_id,
        link=link,
        link_type=link_type.value,
        created_by=created_by,
    )
    if data_slice_id:
        link.data_slice_id = data_slice_id
    if heuristic_id:
        link.heuristic_id = heuristic_id
    if created_at:
        link.created_at = created_at
    general.add(link, with_commit)
    return link


def remove(link_id: str, with_commit: bool = False) -> None:
    session.delete(session.query(LabelingAccessLink).get(link_id))
    general.flush_or_commit(with_commit)


def change_user_access_to_link_lock(
    user_id: str, link_id: str, lock_state: bool, with_commit: bool = False
) -> bool:
    user_link_connection = (
        session.query(UserLinkConnection)
        .filter(
            UserLinkConnection.user_id == user_id, UserLinkConnection.link_id == link_id
        )
        .first()
    )
    if user_link_connection:
        user_link_connection.is_locked = lock_state
        if with_commit:
            general.commit()
        return True
    return False
