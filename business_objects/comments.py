from datetime import datetime
from submodules.model.models import CommentData
from . import general, organization
from .. import User, enums
from ..session import session
from typing import Dict, List, Any, Optional, Union


def get(comment_id: str) -> CommentData:
    return session.query(CommentData).get(comment_id)


def get_by_all_by_project_id(project_id: str) -> List[CommentData]:
    return session.query(CommentData).filter(CommentData.project_id == project_id).all()


def get_by_all_by_category(
    category: enums.CommentCategory,
    user_id: str,
    xfkey: Optional[str] = None,
    project_id: Optional[str] = None,
    as_json: bool = False,
) -> Union[List[CommentData], str]:

    query = f"""
SELECT *
FROM public.comment_data
WHERE xftype = '{category.value}' """

    if xfkey:
        query += f"\n   AND xfkey = '{xfkey}'"
    if project_id:
        query += f"\n   AND project_id = '{project_id}'"

    query += f"\n   AND (is_private = false OR created_by = '{user_id}')"

    if as_json:
        query = f"""
SELECT array_agg(row_to_json(x))
FROM (
    {query}
) x """
        return general.execute_first(query)[0]

    return general.execute_all(query)


def get_by_all_by_xfkey(
    xfkey: str, category: enums.CommentCategory, project_id: Optional[str] = None
) -> List[CommentData]:
    query = session.query(CommentData).filter(
        CommentData.xfkey == xfkey, CommentData.xftype == category.value
    )
    if project_id:
        query = query.filter(CommentData.project_id == project_id)
    return query.all()


def has_comments(
    xftype: enums.CommentCategory,
    xfkey: Optional[str] = None,
    project_id: Optional[str] = None,
    group_by_xfkey: bool = False,
) -> Union[bool, Dict[str, bool]]:
    if group_by_xfkey:
        select = "xfkey::TEXT, COUNT(*)"
    else:
        select = "COUNT(*)"
    query = f"""
SELECT {select}
FROM public.comment_data
WHERE xftype = '{xftype.value}' """
    if xfkey:
        query += f"\n   AND xfkey = '{xfkey}'"
    if project_id:
        query += f"\n   AND project_id = '{project_id}'"
    if group_by_xfkey:
        query += "\nGROUP BY xfkey"

    if not group_by_xfkey:
        return general.execute_first(query)[0] > 0

    return {r[0]: r[1] > 0 for r in general.execute_all(query)}


# should only be used for import export reasons, otherwiese th shorthand version are preferred (e.g. create_for_user)
def create(
    xfkey: str,
    xftype: str,
    comment: str,
    created_by: str,
    project_id: Optional[str] = None,
    order_key: Optional[int] = None,
    is_markdown: Optional[bool] = None,
    is_private: Optional[bool] = None,
    created_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> CommentData:
    comment = CommentData(
        xfkey=xfkey, xftype=xftype, comment=comment, created_by=created_by
    )
    if project_id:
        comment.project_id = project_id
    if order_key:
        comment.order_key = order_key
    if is_markdown:
        comment.is_markdown = is_markdown
    if is_private:
        comment.is_private = is_private
    if created_at:
        comment.created_at = created_at

    general.add(comment, with_commit)
    return comment


def change(
    comment: CommentData,
    changes: Dict[str, Any],
    with_commit: bool = False,
) -> CommentData:
    for k in changes:
        if hasattr(comment, k):
            setattr(comment, k, changes[k])
        else:
            raise ValueError(f"Link has no attribute {k}")
    if with_commit:
        general.commit()


def change_by_id(
    comment_id: str, changes: Dict[str, Any], with_commit: bool = False
) -> CommentData:
    comment = get(comment_id)
    if not comment:
        raise ValueError("comment does not exist")
    change(comment, changes, with_commit)
    return comment


def remove(comment_id: str, with_commit: bool = False) -> None:
    session.delete(session.query(CommentData).get(comment_id))
    general.flush_or_commit(with_commit)
