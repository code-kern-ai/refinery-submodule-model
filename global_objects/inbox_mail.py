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
            InboxMailThread.organization_id == org_id,
        )

    inbox_mail_entities = query.order_by(asc(InboxMail.created_at)).all()
    return inbox_mail_entities


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
        ).filter(
            InboxMailThreadAssociation.user_id == str(user_id),
            InboxMailThread.organization_id == org_id,
        )

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

    thread_dicts = [
        {
            **sql_alchemy_to_dict(thread),
            "latest_mail": sql_alchemy_to_dict(get_first_in_thread(thread.id)),
            "participant_ids": participants_map.get(str(thread.id), []),
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
            )
            thread_association_entities.append(thread_association_entity)
        general.add_all(thread_association_entities)
    else:
        thread_entity = get_inbox_mail_thread_by_id(thread_id)

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
