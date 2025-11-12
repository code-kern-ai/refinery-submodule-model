from typing import Dict, List, Optional, Any
from submodules.model.util import sql_alchemy_to_dict

from ..session import session
from sqlalchemy import cast, String, func, desc

from submodules.model.business_objects import general
from submodules.model.models import InboxMail
from sqlalchemy import or_


def get_by_thread(
    org_id: str,
    user_id: str,
    thread_id: str,
) -> List[InboxMail]:

    return (
        session.query(InboxMail)
        .filter(
            InboxMail.organization_id == org_id,
            InboxMail.thread_id == thread_id,
            or_(
                InboxMail.recipient_id == user_id,
                InboxMail.sender_id == user_id,
            ),
        )
        .order_by(desc(InboxMail.created_at))
        .all()
    )


def get_overview_by_threads(
    org_id: str,
    user_id: str,
    page: int = 1,
    limit: int = 10,
) -> Dict[str, Any]:
    subquery = (
        session.query(
            InboxMail.thread_id,
            func.max(InboxMail.created_at).label("latest_mail_time"),
        )
        .filter(
            InboxMail.organization_id == org_id,
            or_(
                InboxMail.recipient_id == user_id,
                InboxMail.sender_id == user_id,
            ),
        )
        .group_by(InboxMail.thread_id)
        .subquery()
    )

    latest_mails_query = (
        session.query(InboxMail)
        .join(
            subquery,
            (InboxMail.thread_id == subquery.c.thread_id)
            & (InboxMail.created_at == subquery.c.latest_mail_time),
        )
        .order_by(desc(InboxMail.created_at))
    )

    total_threads = latest_mails_query.count()
    latest_mails = latest_mails_query.offset((page - 1) * limit).limit(limit).all()

    threads = []
    for mail in latest_mails:
        total_mails = (
            session.query(func.count(InboxMail.id))
            .filter(
                InboxMail.thread_id == mail.thread_id,
                InboxMail.organization_id == org_id,
            )
            .scalar()
        )

        threads.append(
            {
                "threadId": mail.thread_id,
                "latestMail": sql_alchemy_to_dict(mail),
                "totalMails": total_mails,
            }
        )

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
    mail_entities: List[InboxMail] = []
    for rid in recipient_ids:
        other_recipient_ids = [r for r in recipient_ids if r != rid]

        mail_entity = InboxMail(
            organization_id=org_id,
            sender_id=sender_id,
            recipient_id=rid,
            other_recipient_ids=other_recipient_ids,
            subject=subject,
            content=content,
            meta_data=meta_data or {},
            thread_id=thread_id,
            parent_id=parent_id,
            is_important=is_important,
        )

        mail_entities.append(mail_entity)

    general.add_all(mail_entities)
    if with_commit:
        general.commit()

    return mail_entities
