from typing import Dict, List, Optional, Any

from ..business_objects import general
from ..session import session
from ..models import CustomerButton
from .. import enums
from sqlalchemy.orm.attributes import flag_modified


def get(id: str) -> CustomerButton:
    return (
        session.query(CustomerButton)
        .filter(
            CustomerButton.id == id,
        )
        .first()
    )


def get_all(filter_visible: Optional[bool] = None) -> List[CustomerButton]:
    if not filter_visible:
        return session.query(CustomerButton).all()

    return (
        session.query(CustomerButton)
        .filter(CustomerButton.visible == filter_visible)
        .all()
    )


def get_by_org_id(
    org_id: str, filter_visible: Optional[bool] = None
) -> List[CustomerButton]:
    query = session.query(CustomerButton).filter(
        CustomerButton.org_id == org_id,
    )
    if not filter_visible:
        return query.all()
    return query.filter(CustomerButton.visible == filter_visible).all()


def create(
    org_id: str,
    type: enums.CustomerButtonType,
    location: enums.CustomerButtonLocation,
    config: Dict[str, Any],
    user_id: str,
    visible: bool = False,
    with_commit: bool = True,
) -> CustomerButton:
    obj = CustomerButton(
        organization_id=org_id,
        type=type.value,
        location=location.value,
        config=config,
        visible=visible,
        created_by=user_id,
    )
    general.add(obj, with_commit)

    return obj


def update(
    id: str,
    org_id: str,
    type: Optional[enums.CustomerButtonType] = None,
    location: Optional[enums.CustomerButtonLocation] = None,
    config: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    visible: Optional[bool] = None,
    with_commit: bool = True,
) -> CustomerButton:
    button = get(id)
    if not button or str(org_id) != str(button.organization_id):
        raise ValueError("Button not found or not part of the organization")
    if type:
        button.type = type.value
    if location:
        button
    if config:
        button.config = config
        flag_modified(button, "config")
    if user_id:
        button.created_by = user_id
    if visible:
        button.visible = visible

    general.flush_or_commit(with_commit)
    return button


def delete(id: str, with_commit: bool = True) -> None:
    session.query(CustomerButton).filter(
        CustomerButton.id == id,
    ).delete()
    general.flush_or_commit(with_commit)
