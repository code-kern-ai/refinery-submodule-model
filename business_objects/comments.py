from datetime import datetime
from submodules.model.models import Comment
from . import general, organization
from .. import User, enums
from ..session import session
from typing import Dict, List, Any, Optional, Union


def get(comment_id: str) -> Comment:
    return session.query(Comment).get(comment_id)


def get_by_all_by_project_id(project_id: str) -> List[Comment]:
    return session.query(Comment).filter(Comment.project_id == project_id).all()


def get_by_all_by_category(
    category: enums.CommentCategory,
    xfkey: Optional[str] = None,
    project_id: Optional[str] = None,
    as_json: bool = False,
) -> Union[List[Comment], str]:

    query = f"""
SELECT *
FROM public.comment
WHERE xftype = '{category.value}' """

    if xfkey:
        query += f"\n   AND xfkey = '{xfkey}'"
    if project_id:
        query += f"\n   AND project_id = '{project_id}'"

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
) -> List[Comment]:
    query = session.query(Comment).filter(
        Comment.xfkey == xfkey, Comment.xftype == category.value
    )
    if project_id:
        query = query.filter(Comment.project_id == project_id)
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
FROM public.comment
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
) -> Comment:
    comment = Comment(
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


def create_for_user(
    target_user_id: str,
    creation_user_id: str,
    comment: str,
    is_markdown: Optional[bool] = None,
    with_commit: bool = False,
) -> Comment:
    return create(
        xfkey=target_user_id,
        xftype=enums.CommentCategory.USER.value,
        comment=comment,
        created_by=creation_user_id,
        is_markdown=is_markdown,
        with_commit=with_commit,
    )


def create_for_org(
    target_org_id: str,
    creation_user_id: str,
    comment: str,
    is_markdown: Optional[bool] = None,
    with_commit: bool = False,
) -> Comment:
    return create(
        xfkey=target_org_id,
        xftype=enums.CommentCategory.ORG.value,
        comment=comment,
        created_by=creation_user_id,
        project_id=None,
        add_key=None,
        is_markdown=is_markdown,
        created_at=None,
        with_commit=with_commit,
    )


def create_for_labeling_task(
    project_id: str,
    target_labeling_task_id: str,
    creation_user_id: str,
    comment: str,
    is_markdown: Optional[bool] = None,
    with_commit: bool = False,
) -> Comment:
    return create(
        xfkey=target_labeling_task_id,
        xftype=enums.CommentCategory.LABELING_TASK.value,
        comment=comment,
        created_by=creation_user_id,
        project_id=project_id,
        is_markdown=is_markdown,
        with_commit=with_commit,
    )


def remove(comment_id: str, with_commit: bool = False) -> None:
    session.delete(session.query(Comment).get(comment_id))
    general.flush_or_commit(with_commit)
