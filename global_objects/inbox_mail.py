from concurrent.futures import thread
from typing import Dict, List, Optional, Any
from submodules.model.util import sql_alchemy_to_dict

from ..session import session
from sqlalchemy import cast, String, func, desc, asc

from submodules.model.business_objects import general, user as user_bo
from submodules.model.models import (
    InboxMail,
    InboxMailThread,
    InboxMailThreadAssociation,
)
from sqlalchemy import or_


def get(inbox_mail_id: str) -> InboxMail:
    return session.query(InboxMail).filter(InboxMail.id == inbox_mail_id).first()


def get_new_inbox_mails(
    org_id: str, user_id: str, user_is_admin: bool
) -> Dict[str, int]:
    count_query = (
        session.query(
            InboxMailThreadAssociation.thread_id,
            func.sum(InboxMailThreadAssociation.unread_mail_count).label(
                "total_unread_count"
            ),
        )
        .join(
            InboxMailThread,
            InboxMailThread.id == InboxMailThreadAssociation.thread_id,
        )
        .filter(
            InboxMailThread.organization_id == org_id,
            InboxMailThreadAssociation.user_id == str(user_id),
        )
        .group_by(InboxMailThreadAssociation.thread_id)
    )

    total_unread_count = 0
    for _, thread_unread_count in count_query:
        total_unread_count += thread_unread_count

    if user_is_admin:
        admin_unread_count = 0
        admin_threads = (
            session.query(InboxMailThread)
            .filter(
                InboxMailThread.organization_id == org_id,
                InboxMailThread.is_admin_support_thread == True,
            )
            .all()
        )
        for thread in admin_threads:
            meta_data = thread.meta_data or {}
            admin_unread_count += meta_data.get("unreadMailCountAdmin", 0)
        total_unread_count += admin_unread_count
    return total_unread_count


def get_by_thread(
    org_id: str, user_id: str, thread_id: str, user_is_admin: bool = False
) -> List[InboxMail]:

    # Regular users: only threads they participate in.
    # Admins: threads they participate in (normal), plus all admin support threads.
    query = session.query(InboxMail).join(
        InboxMailThread, InboxMail.thread_id == InboxMailThread.id
    )

    if user_is_admin:
        participant_thread_ids = (
            session.query(InboxMailThreadAssociation.thread_id)
            .filter(InboxMailThreadAssociation.user_id == str(user_id))
            .subquery()
        )
        query = query.filter(
            (InboxMailThread.id == thread_id)
            & (
                (InboxMailThread.id.in_(participant_thread_ids.select()))
                | (InboxMailThread.is_admin_support_thread == True)
            )
        )
    else:
        query = query.join(
            InboxMailThreadAssociation,
            InboxMailThreadAssociation.thread_id == InboxMailThread.id,
        ).filter(
            InboxMailThreadAssociation.user_id == str(user_id),
            InboxMailThread.id == thread_id,
        )

    inbox_mail_entities = query.order_by(asc(InboxMail.created_at)).all()
    return inbox_mail_entities


def get_inbox_mail_thread_length(thread_id: str) -> int:
    count = session.query(InboxMail).filter(InboxMail.thread_id == thread_id).count()
    return count


def delete(inbox_mail_id: str, with_commit: bool = True) -> None:
    session.query(InboxMail).filter(InboxMail.id == inbox_mail_id).delete()
    general.flush_or_commit(with_commit)


def delete_thread_by_id(thread_id: str, with_commit: bool = True) -> None:
    session.query(InboxMailThread).filter(InboxMailThread.id == thread_id).delete()
    general.flush_or_commit(with_commit)


def get_overview_by_threads(
    org_id: str,
    user_id: str,
    page: int = 1,
    limit: int = 10,
    user_is_admin: bool = False,
) -> Dict[str, Any]:

    # Regular users: only threads they participate in.
    # Admins: threads they participate in (normal), plus all admin support threads.

    base_query = session.query(InboxMailThread)

    if user_is_admin:
        participant_thread_ids = (
            session.query(InboxMailThreadAssociation.thread_id)
            .filter(InboxMailThreadAssociation.user_id == str(user_id))
            .subquery()
        )
        query = base_query.filter(
            (InboxMailThread.id.in_(participant_thread_ids.select()))
            | (InboxMailThread.is_admin_support_thread == True)
        )
    else:
        query = base_query.join(
            InboxMailThreadAssociation,
            InboxMailThreadAssociation.thread_id == InboxMailThread.id,
        ).filter(InboxMailThreadAssociation.user_id == str(user_id))

    total_threads = query.count()

    threads = (
        query.order_by(desc(InboxMailThread.created_at))
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    thread_ids = [t.id for t in threads]

    participants = (
        session.query(
            InboxMailThreadAssociation.thread_id, InboxMailThreadAssociation.user_id
        )
        .filter(InboxMailThreadAssociation.thread_id.in_(thread_ids))
        .all()
    )

    participants_map = {}
    for thread_id, participant_user_id in participants:
        participants_map.setdefault(str(thread_id), []).append(str(participant_user_id))

    unread_counts = (
        session.query(
            InboxMailThreadAssociation.thread_id,
            InboxMailThreadAssociation.unread_mail_count,
        )
        .filter(
            InboxMailThreadAssociation.thread_id.in_(thread_ids),
            InboxMailThreadAssociation.user_id == str(user_id),
        )
        .all()
    )
    unread_count_map = {
        str(thread_id): unread_mail_count
        for thread_id, unread_mail_count in unread_counts
    }

    thread_dicts = [
        {
            **sql_alchemy_to_dict(thread),
            "latest_mail": sql_alchemy_to_dict(get_first_in_thread(thread.id)),
            "participant_ids": participants_map.get(str(thread.id), []),
            "unread_mail_count": unread_count_map.get(str(thread.id), 0),
        }
        for thread in threads
    ]

    return {
        "total_threads": total_threads,
        "page": page,
        "limit": limit,
        "threads": thread_dicts,
    }


def get_inbox_mail_thread_by_id(thread_id: str) -> InboxMailThread:
    thread_entity = (
        session.query(InboxMailThread).filter(InboxMailThread.id == thread_id).first()
    )
    if not thread_entity:
        raise ValueError("Inbox mail thread not found")
    return thread_entity


def get_inbox_mail_thread_association_by_thread_id_and_user_id(
    thread_id: str, user_id: str
) -> Optional[InboxMailThreadAssociation]:
    association_entity = (
        session.query(InboxMailThreadAssociation)
        .filter(
            InboxMailThreadAssociation.thread_id == thread_id,
            InboxMailThreadAssociation.user_id == user_id,
        )
        .first()
    )
    return association_entity


def get_inbox_mail_thread_associations_by_thread_id(
    thread_id: str,
) -> List[InboxMailThreadAssociation]:
    association_entities = (
        session.query(InboxMailThreadAssociation)
        .filter(
            InboxMailThreadAssociation.thread_id == thread_id,
        )
        .all()
    )
    return association_entities


def create_by_thread(
    org_id: str,
    sender_id: str,
    content: str,
    recipient_ids: Optional[List[str]] = None,
    subject: Optional[str] = None,
    meta_data: Optional[Dict] = None,
    thread_id: Optional[str] = None,
    is_important: bool = False,
    is_admin_support_thread: bool = False,
    created_by: Optional[str] = None,
    with_commit: bool = True,
) -> List[InboxMail]:

    if thread_id is None:
        if is_admin_support_thread:
            meta_data = meta_data or {}
            meta_data["unreadMailCountAdmin"] = 1
        thread_entity = InboxMailThread(
            created_by=sender_id if not created_by else created_by,
            organization_id=org_id,
            subject=subject,
            meta_data=meta_data or {},
            is_important=is_important,
            is_admin_support_thread=is_admin_support_thread,
        )
        general.add(thread_entity)

        participant_ids = [sender_id] + recipient_ids
        thread_association_entities = []
        for user_id in participant_ids:
            thread_association_entity = InboxMailThreadAssociation(
                thread_id=thread_entity.id,
                user_id=user_id,
                unread_mail_count=1 if user_id != sender_id else 0,
            )
            thread_association_entities.append(thread_association_entity)
        general.add_all(thread_association_entities)
    else:
        thread_entity = get_inbox_mail_thread_by_id(thread_id)
        # Only update unread counts if the sender is also the issue creator
        if is_admin_support_thread and thread_entity.created_by == sender_id:
            meta_data = thread_entity.meta_data or {}
            meta_data["unreadMailCountAdmin"] = (
                meta_data.get("unreadMailCountAdmin", 0) + 1
            )
            thread_entity.meta_data = meta_data
        association_entities = get_inbox_mail_thread_associations_by_thread_id(
            thread_id
        )
        for assoc in association_entities:
            if assoc.user_id != sender_id:
                assoc.unread_mail_count += 1
        general.flush_or_commit(with_commit)

    inbox_mail_entitiy = InboxMail(
        content=content, sender_id=sender_id, thread_id=thread_entity.id
    )

    general.add(inbox_mail_entitiy)

    if with_commit:
        general.commit()

    return inbox_mail_entitiy


def get_first_in_thread(thread_id: str) -> Optional[InboxMail]:
    inbox_mail_entity = (
        session.query(InboxMail)
        .filter(InboxMail.thread_id == thread_id)
        .order_by(asc(InboxMail.created_at))
        .first()
    )
    return inbox_mail_entity


def get_participant_ids_by_thread_id(thread_id: str) -> List[str]:
    associations = (
        session.query(InboxMailThreadAssociation)
        .filter(InboxMailThreadAssociation.thread_id == thread_id)
        .all()
    )
    participant_ids = [assoc.user_id for assoc in associations]
    return participant_ids


def update_thread_progress(
    thread_id: str, is_in_progress: bool, with_commit: bool = True
) -> Dict[str, Any]:
    thread_entity = get_inbox_mail_thread_by_id(thread_id)
    thread_entity.is_in_progress = is_in_progress
    general.flush_or_commit(with_commit)
