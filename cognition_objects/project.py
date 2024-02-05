from typing import List, Optional
from ..business_objects import general
from ..session import session
from ..models import CognitionProject
from .. import enums
from datetime import datetime


def get(project_id: str) -> CognitionProject:
    return (
        session.query(CognitionProject)
        .filter(CognitionProject.id == project_id)
        .first()
    )


def get_all(org_id) -> List[CognitionProject]:
    return (
        session.query(CognitionProject)
        .filter(CognitionProject.organization_id == org_id)
        .order_by(CognitionProject.created_at.asc())
        .all()
    )


def get_all_all() -> List[CognitionProject]:
    return session.query(CognitionProject).all()


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
    with_commit: bool = True,
) -> CognitionProject:
    project: CognitionProject = get(project_id)
    if name is not None:
        project.name = name
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
    general.flush_or_commit(with_commit)
    return project


def delete(project_id: str, with_commit: bool = True) -> None:
    session.query(CognitionProject).filter(
        CognitionProject.id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
