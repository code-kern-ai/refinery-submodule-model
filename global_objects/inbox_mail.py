from typing import Dict, List, Optional, Any
from submodules.model.util import sql_alchemy_to_dict

from ..session import session
from sqlalchemy import cast, String, func, desc, asc

from submodules.model.business_objects import general
from submodules.model.models import InboxMail, InboxMailReference
from sqlalchemy import or_
from submodules.model.enums import InboxMailReferenceScope


def get_by_thread(
    org_id: str,
    user_id: str,
    thread_id: str,
) -> List[InboxMail]:

    inbox_mail_entities = (
        session.query(InboxMail)
        .join(InboxMailReference, InboxMail.id == InboxMailReference.inbox_mail_id)
        .filter(
            InboxMail.organization_id == org_id,
            InboxMail.thread_id == thread_id,
            InboxMailReference.user_id == user_id,
        )
        .order_by(asc(InboxMail.created_at))
        .all()
    )

    return inbox_mail_entities


def get_overview_by_threads(
    org_id: str,
    user_id: str,
    page: int = 1,
    limit: int = 10,
) -> Dict[str, Any]:
    subquery = (
        session.query(
            InboxMail.thread_id,
            func.max(InboxMail.created_at).label("latest_created_at"),
        )
        .join(InboxMailReference, InboxMail.id == InboxMailReference.inbox_mail_id)
        .filter(
            InboxMail.organization_id == org_id,
            InboxMailReference.user_id == user_id,
        )
        .group_by(InboxMail.thread_id)
        .subquery()
    )
    total_threads = session.query(func.count()).select_from(subquery).scalar()
    thread_summaries = (
        session.query(InboxMail)
        .join(
            subquery,
            (InboxMail.thread_id == subquery.c.thread_id)
            & (InboxMail.created_at == subquery.c.latest_created_at),
        )
        .order_by(desc(InboxMail.created_at))
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    threads = [
        {"id": str(mail.thread_id), "latest_mail": {**sql_alchemy_to_dict(mail)}}
        for mail in thread_summaries
    ]

    return {
        "totalThreads": total_threads,
        "page": page,
        "limit": limit,
        "threads": threads,
    }


def create_by_thread(
    org_id: str,
    sender_id: str,
    recipient_ids: List[str],
    subject: str,
    content: str,
    meta_data: Optional[Dict] = None,
    parent_id: Optional[str] = None,
    thread_id: Optional[str] = None,
    is_important: bool = False,
    with_commit: bool = True,
) -> List[InboxMail]:
    inbox_mail_entitiy = InboxMail(
        organization_id=org_id,
        sender_id=sender_id,
        original_recipient_ids=recipient_ids,
        subject=subject,
        content=content,
        meta_data=meta_data or {},
        parent_id=parent_id,
        thread_id=thread_id,
        is_important=is_important,
    )

    general.add(inbox_mail_entitiy)

    inbox_mail_references = []

    inbox_mail_sender_reference = InboxMailReference(
        inbox_mail_id=inbox_mail_entitiy.id,
        user_id=sender_id,
        scope=InboxMailReferenceScope.SENDER.value,
        is_seen=True,
    )
    inbox_mail_references.append(inbox_mail_sender_reference)

    for rid in recipient_ids:
        inbox_mail_references.append(
            InboxMailReference(
                inbox_mail_id=inbox_mail_entitiy.id,
                user_id=rid,
                scope=InboxMailReferenceScope.RECIPIENT.value,
            )
        )
    general.add_all(inbox_mail_references)

    if with_commit:
        general.commit()

    return inbox_mail_entitiy
