from datetime import datetime
from typing import List, Dict, Optional, Union

from submodules.model import enums


from ..session import session
from ..models import Organization, Project
from ..business_objects import project, user, general


def get(id: str) -> Organization:
    return session.query(Organization).get(id)


def get_by_name(name: str) -> Organization:
    return session.query(Organization).filter(Organization.name == name).first()


def get_id_by_project_id(project_id: str) -> str:
    project_item = session.query(Project).filter(Project.id == project_id).first()
    if project_item:
        return str(project_item.organization_id)


def get_all() -> List[Organization]:
    return session.query(Organization).all()


def get_organization_id(project_id: str, user_id: str) -> str:
    if project_id:
        project_item = project.get(project_id)
        if project_item:
            return str(project_item.organization_id)
    user_item = user.get(user_id)
    if user_item:
        return str(user_item.organization_id)
    return None


def get_organization_overview_stats(
    organization_id: str,
) -> List[Dict[str, Union[str, int]]]:
    values = general.execute_first(
        __get_organization_overview_stats_query(organization_id)
    )
    if values:
        return values[0]


def __get_organization_overview_stats_query(organization_id: str):
    return f"""
    WITH labeled_records AS (
    SELECT project_id, source_type, COUNT(*) source_records
    FROM (
        SELECT rla.project_id, rla.record_id, rla.source_type
        FROM record r
        INNER JOIN record_label_association rla
            ON r.project_id = rla.project_id AND r.id = rla.record_id AND r.category = '{enums.RecordCategory.SCALE.value}'
        INNER JOIN project p
            ON rla.project_id = p.id
        WHERE p.organization_id = '{organization_id}'
        AND rla.source_type IN ('{enums.LabelSource.MANUAL.value}', '{enums.LabelSource.WEAK_SUPERVISION.value}')
        GROUP BY rla.project_id, rla.record_id, rla.source_type
    ) r_reduction
    GROUP BY project_id, source_type)

    SELECT array_agg(row_to_json(x))
    FROM (
        SELECT 
            base.project_id "projectId", 
            base.base_count "numDataScaleUploaded", 
            COALESCE(lr_m.source_records,0) "numDataScaleManual", 
            COALESCE(lr_w.source_records,0) "numDataScaleProgrammatical"
        FROM (
            SELECT r.project_id, COUNT(*) base_count
            FROM project p
            LEFT JOIN record r
                ON r.project_id = p.id
            WHERE p.organization_id = '{organization_id}'
            AND p."status" != '{enums.ProjectStatus.IN_DELETION.value}'
            AND r.category = '{enums.RecordCategory.SCALE.value}'
            GROUP BY r.project_id
        ) base
        LEFT JOIN labeled_records lr_m
            ON base.project_id = lr_m.project_id AND lr_m.source_type = '{enums.LabelSource.MANUAL.value}'
        LEFT JOIN labeled_records lr_w
            ON base.project_id = lr_w.project_id AND lr_w.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}' )x
    """


def create(
    name: str,
    started_at: Optional[datetime] = None,
    is_paying: Optional[bool] = None,
    with_commit: bool = False,
) -> Organization:
    organization = Organization(name=name)
    if started_at:
        organization.started_at = started_at
    if is_paying:
        organization.is_paying = is_paying
    general.add(organization, with_commit)
    return organization


def delete_by_name(name: str, with_commit: bool = False) -> None:
    session.query(Organization).filter(Organization.name == name).delete()
    general.flush_or_commit(with_commit)


def delete(id: str, with_commit: bool = False) -> None:
    session.query(Organization).filter(Organization.id == id).delete()
    general.flush_or_commit(with_commit)
