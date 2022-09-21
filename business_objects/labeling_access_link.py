from datetime import datetime
from submodules.model.models import InformationSource, LabelingAccessLink
from . import general
from .. import enums
from ..session import session
from typing import Any, Dict, List, Optional
from sqlalchemy import sql
from sqlalchemy import or_, and_


def get(link_id: str) -> LabelingAccessLink:
    return session.query(LabelingAccessLink).get(link_id)


def get_by_link(project_id: str, link: str) -> LabelingAccessLink:
    index_parameter = link.find("?")
    if index_parameter > -1:
        link = link[:index_parameter]
    link += "%"
    return (
        session.query(LabelingAccessLink)
        .filter(
            LabelingAccessLink.project_id == project_id,
            LabelingAccessLink.link.like(link),
        )
        .first()
    )


def get_ensure_access(link_id: str) -> LabelingAccessLink:
    link = (
        session.query(LabelingAccessLink)
        .filter(LabelingAccessLink.id == link_id, LabelingAccessLink.is_locked == False)
        .first()
    )
    if not link:
        raise Exception("No access to this link")
    return link


def get_all_by_type_and_external_id(
    project_id: str, type: enums.LinkTypes, id: str
) -> List[LabelingAccessLink]:

    query = session.query(LabelingAccessLink).filter(
        LabelingAccessLink.project_id == project_id,
    )
    if type == enums.LinkTypes.DATA_SLICE:
        add_link_ids = __get_add_ids_data_slice(project_id, id)
        if add_link_ids:
            query = query.filter(
                or_(
                    and_(
                        LabelingAccessLink.data_slice_id == id,
                        LabelingAccessLink.link_type == type.value,
                    ),
                    LabelingAccessLink.id.in_(add_link_ids),
                )
            )
        else:
            query = query.filter(
                LabelingAccessLink.data_slice_id == id,
                LabelingAccessLink.link_type == type.value,
            )
    elif type == enums.LinkTypes.HEURISTIC:
        query = query.filter(
            LabelingAccessLink.heuristic_id == id,
            LabelingAccessLink.link_type == type.value,
        )

    return query.all()


def __get_add_ids_data_slice(project_id: str, slice_id: str) -> List[str]:
    query = f"""
    SELECT  array_agg(lal.id::TEXT)
    FROM labeling_access_link lal
    INNER JOIN information_source _is
        ON lal.project_id = _is.project_id AND lal.heuristic_id = _is.id
    WHERE lal.project_id = '{project_id}' 
        AND _is.source_code::JSON->>'data_slice_id' = '{slice_id}'
        AND _is.type = '{enums.InformationSourceType.CROWD_LABELER.value}' """
    add_ids = general.execute_first(query)
    if add_ids:
        return add_ids[0]


def get_by_all_by_project_id(project_id: str) -> List[LabelingAccessLink]:
    return (
        session.query(LabelingAccessLink)
        .filter(LabelingAccessLink.project_id == project_id)
        .all()
    )


def get_by_all_by_user_id(
    project_id: str, user_id: str, user_role: enums.UserRoles
) -> List[LabelingAccessLink]:
    if user_role == enums.UserRoles.ANNOTATOR:
        query = f"""
        SELECT _is.id::TEXT
        FROM labeling_access_link lal
        INNER JOIN information_source _is
            ON lal.project_id = _is.project_id AND lal.heuristic_id = _is.id
        WHERE _is.source_code::JSON->>'annotator_id' = '{user_id}'
            AND _is.type = '{enums.InformationSourceType.CROWD_LABELER.value}'
            AND NOT lal.is_locked
        """
        ids = [r[0] for r in general.execute_all(query)]

        return (
            session.query(LabelingAccessLink)
            .filter(
                LabelingAccessLink.is_locked == False,
                LabelingAccessLink.project_id == project_id,
                LabelingAccessLink.heuristic_id.in_(ids),
            )
            .all()
        )
    else:
        return (
            session.query(LabelingAccessLink)
            .filter(
                LabelingAccessLink.is_locked == False,
                LabelingAccessLink.project_id == project_id,
                LabelingAccessLink.data_slice_id != None,
            )
            .all()
        )


def create(
    project_id: str,
    link: str,
    link_type: enums.LinkTypes,
    created_by: str,
    data_slice_id: Optional[str] = None,
    heuristic_id: Optional[str] = None,
    created_at: Optional[datetime] = None,
    changed_at: Optional[datetime] = None,
    with_commit: bool = False,
) -> LabelingAccessLink:

    link = LabelingAccessLink(
        project_id=project_id,
        link=link,
        link_type=link_type.value,
        created_by=created_by,
    )
    if data_slice_id:
        link.data_slice_id = data_slice_id
    if heuristic_id:
        link.heuristic_id = heuristic_id
    if created_at:
        link.created_at = created_at
    if changed_at:
        link.changed_at = changed_at
    general.add(link, with_commit)
    return link


def change_by_id(
    link_id: str, changes: Optional[Dict[str, Any]] = None, with_commit: bool = False
) -> LabelingAccessLink:
    link = get(link_id)
    if not link:
        raise ValueError("Link does not exist")
    change(link, changes, with_commit)
    return link


def change(
    link: LabelingAccessLink,
    changes: Optional[Dict[str, Any]] = None,
    with_commit: bool = False,
) -> LabelingAccessLink:
    if changes:
        for k in changes:
            if hasattr(link, k):
                setattr(link, k, changes[k])
            else:
                raise ValueError(f"Link has no attribute {k}")
    # changes in the data slice should also be represented in the changed_at timestamp
    link.changed_at = sql.func.now()
    if with_commit:
        general.commit()


def remove(link_id: str, with_commit: bool = False) -> None:
    session.delete(get(link_id))
    general.flush_or_commit(with_commit)
