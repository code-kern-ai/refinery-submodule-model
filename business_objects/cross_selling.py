from datetime import datetime
from typing import List, Optional, Union

from . import general
from ..session import session
from ..models import CrossSelling
from ..enums import CrossSellingFilter, try_parse_enum_value
from ..util import prevent_sql_injection


def build_cross_selling_filter_sql(
    cross_selling_filter: Optional[Union[str, CrossSellingFilter]],
    table_alias: str = "o",
) -> str:
    if cross_selling_filter is None:
        return ""
    if cross_selling_filter and not isinstance(
        cross_selling_filter, CrossSellingFilter
    ):
        cross_selling_filter = prevent_sql_injection(
            cross_selling_filter, isinstance(cross_selling_filter, str)
        )
    _cs_filter: Union[str, CrossSellingFilter] = cross_selling_filter
    if isinstance(cross_selling_filter, str):
        parsed = try_parse_enum_value(
            cross_selling_filter, CrossSellingFilter, raise_me=False
        )
        _cs_filter = parsed if parsed is not None else cross_selling_filter
    col_ref = f"{table_alias}.cross_selling_id"
    if _cs_filter == CrossSellingFilter.HAS_CROSS_SELLING:
        return f"{col_ref} IS NOT NULL"
    if _cs_filter == CrossSellingFilter.NO_CROSS_SELLING:
        return f"{col_ref} IS NULL"
    if (
        cross_selling_filter
        and _cs_filter != CrossSellingFilter.NO_FILTER
        and isinstance(cross_selling_filter, str)
    ):
        return f"{col_ref} = '{cross_selling_filter}'"
    return ""


def get(cross_selling_id: str) -> Optional[CrossSelling]:
    return (
        session.query(CrossSelling).filter(CrossSelling.id == cross_selling_id).first()
    )


def get_all() -> List[CrossSelling]:
    return session.query(CrossSelling).order_by(CrossSelling.created_at.desc()).all()


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
