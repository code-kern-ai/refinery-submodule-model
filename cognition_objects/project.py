from typing import List, Optional, Dict, Any, Iterable
from ..business_objects import general, team_resource, user
from ..cognition_objects import consumption_log, consumption_summary
from ..session import session
from ..models import CognitionProject, TeamMember, TeamResource
from .. import enums
from datetime import datetime
from ..util import prevent_sql_injection
from sqlalchemy.orm.attributes import flag_modified
from copy import deepcopy


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


def get_lookup_by_ids(ids: Iterable[str]) -> Dict[str, CognitionProject]:
    return {
        str(e.id): e
        for e in session.query(CognitionProject)
        .filter(CognitionProject.id.in_(ids))
        .all()
    }


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
        WHERE u.organization_id = '{organization_id}'
        AND NOT (t.project_id IS NULL AND u.role = '{enums.UserRoles.ANNOTATOR.value}')
        AND NOT u.role = '{enums.UserRoles.EXPERT.value}'
        GROUP BY 1,2 )x """
    values = general.execute_first(query)
    if values and values[0]:
        return values[0]
    return {}


ROUTING_SOURCE_CODE_DEFAULT_BLANK = """from typing import Dict, Any, Tuple
def routing(
    record_dict: Dict[str, Any], scope_dict: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    if "answer" in record_dict:
        record_dict['routing'] = 'STOP'
        return record_dict, scope_dict
    record_dict['routing'] = 'Plain LLM'
    return record_dict, scope_dict
"""


DEFAULT_MACRO_CONFIG = {
    "enable": False,
    "show": enums.AdminMacrosDisplay.DONT_SHOW.value,
}


def create(
    name: str,
    description: str,
    color: str,
    org_id: str,
    user_id: str,
    interface_type: str,
    tokenizer: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    operator_routing_config: Optional[Dict[str, Any]] = None,
    macro_config: Optional[Dict[str, Any]] = None,
) -> CognitionProject:
    if macro_config is None:
        macro_config = DEFAULT_MACRO_CONFIG
    if operator_routing_config is None:
        operator_routing_config = {"sourceCode": ROUTING_SOURCE_CODE_DEFAULT_BLANK}
    project: CognitionProject = CognitionProject(
        name=name,
        description=description,
        color=color,
        organization_id=org_id,
        created_by=user_id,
        created_at=created_at,
        interface_type=interface_type,
        operator_routing_config=operator_routing_config,
        macro_config=macro_config,
        tokenizer=tokenizer,
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
    operator_routing_config: Optional[Dict[str, Any]] = None,
    state: Optional[enums.CognitionProjectState] = None,
    facts_grouping_attribute: Optional[str] = None,
    allow_file_upload: Optional[bool] = None,
    max_file_size_mb: Optional[float] = None,
    max_folder_size_mb: Optional[float] = None,
    macro_config: Optional[Dict[str, Any]] = None,
    llm_config: Optional[Dict[str, Any]] = None,
    tokenizer: Optional[str] = None,
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
    if state is not None:
        project.state = state.value
    if facts_grouping_attribute is not None:
        project.facts_grouping_attribute = facts_grouping_attribute
    if allow_file_upload is not None:
        project.allow_file_upload = allow_file_upload
    if max_file_size_mb is not None:
        project.max_file_size_mb = max_file_size_mb
    if max_folder_size_mb is not None:
        project.max_folder_size_mb = max_folder_size_mb
    if macro_config is not None:
        new_values = project.macro_config
        if new_values is None:
            new_values = deepcopy(DEFAULT_MACRO_CONFIG)
        for key in macro_config:
            new_values[key] = macro_config[key]

        project.macro_config = new_values
        flag_modified(project, "macro_config")
    if llm_config is not None:
        new_values = project.llm_config
        if new_values is None:
            new_values = {}

        # if level 3+ depth is needed, we will need to extend below using deepcopy
        for key in llm_config:
            if isinstance(llm_config[key], dict):
                if key not in new_values:
                    new_values[key] = {}
                for sub_key in llm_config[key]:
                    if llm_config[key][sub_key] == "_null":
                        if sub_key in new_values[key]:
                            del new_values[key][sub_key]
                    else:
                        new_values[key][sub_key] = llm_config[key][sub_key]
            else:
                new_values[key] = llm_config[key]
        project.llm_config = new_values
        flag_modified(project, "llm_config")

    if operator_routing_config is not None:
        new_values = project.operator_routing_config
        if new_values is None:
            new_values = deepcopy({"sourceCode": ROUTING_SOURCE_CODE_DEFAULT_BLANK})
        for key in operator_routing_config:
            if isinstance(operator_routing_config[key], dict):
                if key not in new_values:
                    new_values[key] = {}
                for sub_key in operator_routing_config[key]:
                    new_values[key][sub_key] = operator_routing_config[key][sub_key]
            new_values[key] = operator_routing_config[key]

        project.operator_routing_config = new_values
        flag_modified(project, "operator_routing_config")
    if tokenizer is not None:
        project.tokenizer = tokenizer
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
