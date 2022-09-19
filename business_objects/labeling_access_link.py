from datetime import datetime
from submodules.model.models import LabelingAccessLink
from . import general
from .. import enums
from ..session import session
from typing import List, Optional


def get(link_id: str) -> LabelingAccessLink:
    return session.query(LabelingAccessLink).get(link_id)


def get_by_link(project_id: str, link: str) -> LabelingAccessLink:
    return (
        session.query(LabelingAccessLink)
        .filter(
            LabelingAccessLink.project_id == project_id, LabelingAccessLink.link == link
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


def get_by_all_by_project_id(project_id: str) -> List[LabelingAccessLink]:
    return (
        session.query(LabelingAccessLink)
        .filter(LabelingAccessLink.project_id == project_id)
        .all()
    )


def get_by_all_by_user_id(
    user_id: str, user_role: enums.UserRoles
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
        ids = [r[id] for r in general.execute_all(query)]

        return (
            session.query(LabelingAccessLink)
            .filter(
                LabelingAccessLink.is_locked == False,
                LabelingAccessLink.heuristic_id.in_(ids),
            )
            .all()
        )
    else:
        (
            session.query(LabelingAccessLink)
            .filter(
                LabelingAccessLink.is_locked == False,
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
    general.add(link, with_commit)
    return link


def remove(link_id: str, with_commit: bool = False) -> None:
    session.delete(get(link_id))
    general.flush_or_commit(with_commit)
