from datetime import datetime
from typing import List, Optional

from . import general
from ..session import session
from ..models import CrossSelling


def get(cross_selling_id: str) -> Optional[CrossSelling]:
    return (
        session.query(CrossSelling)
        .filter(CrossSelling.id == cross_selling_id)
        .first()
    )


def get_all() -> List[CrossSelling]:
    return (
        session.query(CrossSelling)
        .order_by(CrossSelling.created_at.desc())
        .all()
    )


def create(
    name: Optional[str] = None,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> CrossSelling:
    cross_selling = CrossSelling(name=name, created_at=created_at)
    general.add(cross_selling, with_commit)
    return cross_selling


def update(
    cross_selling_id: str,
    name: Optional[str] = None,
    with_commit: bool = True,
) -> Optional[CrossSelling]:
    cross_selling = get(cross_selling_id)
    if not cross_selling:
        return None
    if name is not None:
        cross_selling.name = name
    general.flush_or_commit(with_commit)
    return cross_selling


def delete(cross_selling_id: str, with_commit: bool = False) -> None:
    cross_selling = get(cross_selling_id)
    if cross_selling:
        general.delete(cross_selling, with_commit)
