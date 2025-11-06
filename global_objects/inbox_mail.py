from typing import Dict, List

from ..session import session
from sqlalchemy import cast, String

from submodules.model.business_objects import general
from submodules.model.models import InboxMail


def get_inbox_mail(
    org_id: str,
    user_email: str,
) -> List[InboxMail]:
    return (
        session.query(InboxMail)
        .filter(InboxMail.organization_id == org_id)
        .filter(cast(InboxMail.send_to, String).like(f"%{user_email}%"))
        .all()
    )


def create(
    org_id: str,
    send_from: str,
    send_to: Dict,
    subject: str,
    content: str,
    mark_as_important: bool,
    meta_data: Dict,
    parent_id: str = None,
    child_id: str = None,
    with_commit: bool = True,
) -> InboxMail:
    obj = InboxMail(
        organization_id=org_id,
        send_from=send_from,
        send_to=send_to,
        subject=subject,
        content=content,
        mark_as_important=mark_as_important,
        meta_data=meta_data,
        parent_id=parent_id,
        child_id=child_id,
    )
    general.add(obj, with_commit)

    return obj
