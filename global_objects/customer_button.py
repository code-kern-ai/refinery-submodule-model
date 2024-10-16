from typing import Dict, List, Optional, Any

from ..business_objects import general
from ..session import session
from ..models import CustomerButton
from ..util import prevent_sql_injection
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

    where_add = ""

    if filter_visible is not None:
        filter_visible = prevent_sql_injection(
            filter_visible, isinstance(filter_visible, bool)
        )
        where_add = f"WHERE cb.visible = {filter_visible}"

    query = f"""
        SELECT cb.*, o.name
        FROM global.customer_button cb
        INNER JOIN public.organization o
            ON cb.organization_id = o.id
        {where_add}
    """
    return general.execute_all(query)


def get_by_org_id(
    org_id: str,
    filter_visible: Optional[bool] = None,
    filter_location: Optional[enums.CustomerButtonLocation] = None,
) -> List[CustomerButton]:
    # org name only relevant for admin dashboard, this is for org specific so no need to join
    query = session.query(CustomerButton).filter(
        CustomerButton.organization_id == org_id,
    )

    if filter_visible is not None:
        query = query.filter(CustomerButton.visible == filter_visible)
    if filter_location is not None:
        query = query.filter(CustomerButton.location == filter_location.value)

    return query.all()


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
        button.location = location.value
    if config:
        button.config = config
        flag_modified(button, "config")
    if user_id:
        button.created_by = user_id
    if visible is not None:
        button.visible = visible
    general.flush_or_commit(with_commit)
    return button


def delete(id: str, with_commit: bool = True) -> None:
    session.query(CustomerButton).filter(
        CustomerButton.id == id,
    ).delete()
    general.flush_or_commit(with_commit)
