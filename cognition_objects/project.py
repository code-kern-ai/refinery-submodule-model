from typing import List, Optional, Dict
from ..business_objects import general, team_resource, user
from ..cognition_objects import consumption_log, consumption_summary
from ..session import session
from ..models import CognitionProject, TeamMember, TeamResource
from .. import enums
from datetime import datetime
from ..util import prevent_sql_injection


def get(project_id: str) -> CognitionProject:
    return (
        session.query(CognitionProject)
        .filter(CognitionProject.id == project_id)
        .first()
    )


def get_org_id(project_id: str) -> str:
    if p := get(project_id):
        return str(p.organization_id)
    raise ValueError(f"Project with id {project_id} not found")


def get_by_user(project_id: str, user_id: str) -> CognitionProject:
    user_item = user.get(user_id)
    if user_item.role == enums.UserRoles.ENGINEER.value:
        return get(project_id)

    return (
        session.query(CognitionProject)
        .join(TeamResource, TeamResource.resource_id == CognitionProject.id)
        .join(TeamMember, TeamMember.team_id == TeamResource.team_id)
        .filter(TeamMember.user_id == user_id)
        .filter(
            TeamResource.resource_type == enums.TeamResourceType.COGNITION_PROJECT.value
        )
        .filter(CognitionProject.id == project_id)
        .first()
    )


def get_all(org_id: str, order_by_name: bool = False) -> List[CognitionProject]:
    query = session.query(CognitionProject).filter(
        CognitionProject.organization_id == org_id
    )

    if order_by_name:
        query = query.order_by(CognitionProject.name.asc())
    else:
        query = query.order_by(CognitionProject.created_at.asc())
    return query.all()


def get_all_all() -> List[CognitionProject]:
    return session.query(CognitionProject).all()


def get_all_by_user(org_id: str, user_id: str) -> List[CognitionProject]:
    user_item = user.get(user_id)
    if user_item.role == enums.UserRoles.ENGINEER.value:
        return get_all(org_id)

    return (
        session.query(CognitionProject)
        .join(TeamResource, TeamResource.resource_id == CognitionProject.id)
        .join(TeamMember, TeamMember.team_id == TeamResource.team_id)
        .filter(TeamMember.user_id == user_id)
        .filter(
            TeamResource.resource_type == enums.TeamResourceType.COGNITION_PROJECT.value
        )
        .filter(CognitionProject.organization_id == org_id)
        .order_by(CognitionProject.created_at.asc())
        .all()
    )


# returns a dict with ENGINEERING_TEAM as key for all users that are not annotators
def get_project_users_overview(
    organization_id: str, project_id: Optional[str] = None
) -> Dict[str, int]:
    organization_id = prevent_sql_injection(
        organization_id, isinstance(organization_id, str)
    )

    p_where = ""
    if project_id:
        project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
        p_where = f"AND tr.resource_id = '{project_id}'"
    # count is distinct to ensure the same user in two different teams (which both have access to the same project) is only counted once
    query = f"""
    SELECT jsonb_object_agg(ind,c)
    FROM (
        SELECT COALESCE(t.project_id::TEXT,'ENGINEERING_TEAM') ind, u.role, count(DISTINCT u.id) c
        FROM public.user u
        LEFT JOIN (
            SELECT 
                tr.resource_id project_id,
                tm.user_id
            FROM team t
            LEFT JOIN team_member tm
                ON t.id = tm.team_id
            LEFT JOIN team_resource tr
                ON t.id = tr.team_id AND tr.resource_type = '{enums.TeamResourceType.COGNITION_PROJECT.value}'
            WHERE t.organization_id = '{organization_id}' {p_where}
        ) t
            ON u.id = t.user_id
        WHERE u.organization_id = '{organization_id}' AND NOT (t.project_id IS NULL AND u.role = 'ANNOTATOR') AND NOT u.role = 'EXPERT'
        GROUP BY 1,2 )x """
    values = general.execute_first(query)
    if values and values[0]:
        return values[0]
    return {}


def get_all_for_synchronization_option(
    org_id: str, synchronization_option: str
) -> List[CognitionProject]:
    return (
        session.query(CognitionProject)
        .filter(CognitionProject.organization_id == org_id)
        .filter(
            CognitionProject.refinery_synchronization_interval_option
            == synchronization_option
        )
        .order_by(CognitionProject.created_at.asc())
        .all()
    )


EXECUTE_QUERY_ENRICHMENT_IF_SOURCE_CODE = """from typing import Dict, Any, Tuple

def check_execute(
    record_dict: Dict[str, Any], scope_dict: Dict[str, Any]
) -> bool:
    return True

"""


def create(
    name: str,
    description: str,
    color: str,
    org_id: str,
    user_id: str,
    interface_type: str,
    refinery_references_project_id: str,
    refinery_queries_project_id: str,
    refinery_relevances_project_id: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionProject:
    operator_routing_source_code = """from typing import Dict, Any, Tuple

def routing(
    record_dict: Dict[str, Any], scope_dict: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    if True: # add a condition here to differentiate when to use what strategy
        record_dict['routing'] = 'Common RAG'
    else:
        record_dict['routing'] = 'Low-code strategy'
    return record_dict, scope_dict

"""

    project: CognitionProject = CognitionProject(
        name=name,
        description=description,
        color=color,
        organization_id=org_id,
        created_by=user_id,
        created_at=created_at,
        interface_type=interface_type,
        refinery_references_project_id=refinery_references_project_id,
        refinery_question_project_id=refinery_queries_project_id,
        refinery_relevance_project_id=refinery_relevances_project_id,
        operator_routing_source_code=operator_routing_source_code,
        refinery_synchronization_interval_option=enums.RefinerySynchronizationIntervalOption.NEVER.value,
        execute_query_enrichment_if_source_code=EXECUTE_QUERY_ENRICHMENT_IF_SOURCE_CODE,
    )
    general.add(project, with_commit)
    return project


def update(
    project_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    customer_color_primary: Optional[str] = None,
    customer_color_primary_only_accent: Optional[bool] = None,
    customer_color_secondary: Optional[str] = None,
    operator_routing_source_code: Optional[str] = None,
    refinery_synchronization_interval_option: Optional[str] = None,
    execute_query_enrichment_if_source_code: Optional[str] = None,
    state: Optional[enums.CognitionProjectState] = None,
    facts_grouping_attribute: Optional[str] = None,
    allow_file_upload: Optional[bool] = None,
    max_file_size_mb: Optional[float] = None,
    open_ai_env_var_id: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionProject:
    project: CognitionProject = get(project_id)
    if name is not None:
        project.name = name
        consumption_summary.update_project_name(project_id, name, with_commit=False)
        consumption_log.update_project_name(project_id, name, with_commit=False)
    if description is not None:
        project.description = description
    if customer_color_primary is not None:
        project.customer_color_primary = customer_color_primary
    if customer_color_primary_only_accent is not None:
        project.customer_color_primary_only_accent = customer_color_primary_only_accent
    if customer_color_secondary is not None:
        project.customer_color_secondary = customer_color_secondary
    if operator_routing_source_code is not None:
        project.operator_routing_source_code = operator_routing_source_code
    if refinery_synchronization_interval_option is not None:
        project.refinery_synchronization_interval_option = (
            refinery_synchronization_interval_option
        )
    if execute_query_enrichment_if_source_code is not None:
        project.execute_query_enrichment_if_source_code = (
            execute_query_enrichment_if_source_code
        )
    if state is not None:
        project.state = state.value
    if facts_grouping_attribute is not None:
        project.facts_grouping_attribute = facts_grouping_attribute
    if allow_file_upload is not None:
        project.allow_file_upload = allow_file_upload
    if max_file_size_mb is not None:
        project.max_file_size_mb = max_file_size_mb
    if open_ai_env_var_id is not None:
        project.open_ai_env_var_id = open_ai_env_var_id
    general.flush_or_commit(with_commit)
    return project


def delete(project_id: str, with_commit: bool = True) -> None:
    team_resource.delete_by_resource(
        project_id, enums.TeamResourceType.COGNITION_PROJECT, with_commit=False
    )
    session.query(CognitionProject).filter(
        CognitionProject.id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
