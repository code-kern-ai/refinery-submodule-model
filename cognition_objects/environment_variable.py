from typing import List, Optional, Dict
from datetime import datetime
from ..business_objects import general
from . import project as cognition_project
from ..session import session
from ..models import (
    CognitionEnvironmentVariable,
    CognitionMarkdownFile,
    CognitionMarkdownDataset,
    CognitionProject,
)
from ..util import prevent_sql_injection
from sqlalchemy import or_
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql.expression import cast


def get(project_id: str, environment_variable_id: str) -> CognitionEnvironmentVariable:
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(
            CognitionEnvironmentVariable.project_id == project_id,
            CognitionEnvironmentVariable.id == environment_variable_id,
        )
        .first()
    )


def get_by_name(
    project_id: str,
    name: str,
) -> CognitionEnvironmentVariable:
    org_id = cognition_project.get(project_id)
    if not org_id:
        raise ValueError("Couldn't find project in organization")
    org_id = str(org_id.organization_id)
    # depending on the scope the org_id or project_id are empty so for a get_by_name we need to check both
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(
            or_(
                CognitionEnvironmentVariable.project_id == project_id,
                CognitionEnvironmentVariable.organization_id == org_id,
            )
        )
        .filter(CognitionEnvironmentVariable.name == name)
        .first()
    )


def get_by_name_and_org_id(
    org_id: str,
    name: str,
) -> CognitionEnvironmentVariable:

    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.organization_id == org_id)
        .filter(CognitionEnvironmentVariable.project_id == None)
        .filter(CognitionEnvironmentVariable.name == name)
        .first()
    )


def get_by_md_file_id(md_file_id: str) -> CognitionEnvironmentVariable:
    env_var_id = cast(CognitionMarkdownDataset.llm_config.op("->>")("envVarId"), UUID)
    return (
        session.query(CognitionEnvironmentVariable)
        .join(
            CognitionMarkdownDataset,
            env_var_id == CognitionEnvironmentVariable.id,
        )
        .join(
            CognitionMarkdownFile,
            CognitionMarkdownFile.dataset_id == CognitionMarkdownDataset.id,
        )
        .filter(
            CognitionMarkdownFile.id == md_file_id,
        )
        .first()
    )


def get_dataset_env_var_value(dataset_id: str, org_id) -> CognitionEnvironmentVariable:

    env_var_id = cast(CognitionMarkdownDataset.llm_config.op("->>")("envVarId"), UUID)
    return (
        session.query(CognitionEnvironmentVariable)
        .join(
            CognitionMarkdownDataset,
            env_var_id == CognitionEnvironmentVariable.id,
        )
        .filter(CognitionEnvironmentVariable.organization_id == org_id)
        .filter(
            CognitionMarkdownDataset.id == dataset_id,
        )
        .first()
    )


def get_dataset_azure_models_env_var_value(
    dataset_id: str, org_id
) -> CognitionEnvironmentVariable:

    env_var_id = cast(
        CognitionMarkdownDataset.llm_config.op("->>")("azureModelsEnvVarId"), UUID
    )
    return (
        session.query(CognitionEnvironmentVariable)
        .join(
            CognitionMarkdownDataset,
            env_var_id == CognitionEnvironmentVariable.id,
        )
        .filter(CognitionEnvironmentVariable.organization_id == org_id)
        .filter(
            CognitionMarkdownDataset.id == dataset_id,
        )
        .first()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionEnvironmentVariable]:
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.project_id == project_id)
        .filter(CognitionEnvironmentVariable.organization_id == None)
        .order_by(CognitionEnvironmentVariable.created_at.asc())
        .all()
    )


def get_cognition_project_env_var_value(cognition_project_id: str) -> str:

    env_var_id = cast(CognitionProject.llm_config.op("->>")("envVarId"), UUID)
    v = (
        session.query(CognitionEnvironmentVariable.value)
        .join(CognitionProject, env_var_id == CognitionEnvironmentVariable.id)
        .filter(
            CognitionProject.id == cognition_project_id,
        )
        .first()
    )
    if v:
        return str(v[0])


def get_cognition_project_azure_models_env_var_value(cognition_project_id: str) -> str:
    env_var_id = cast(
        CognitionProject.llm_config.op("->>")("azureModelsEnvVarId"), UUID
    )
    v = (
        session.query(CognitionEnvironmentVariable.value)
        .join(CognitionProject, env_var_id == CognitionEnvironmentVariable.id)
        .filter(
            CognitionProject.id == cognition_project_id,
        )
        .first()
    )
    if v:
        return str(v[0])


def get_all_by_org_id(org_id: str) -> List[CognitionEnvironmentVariable]:
    # Note, atm this doesn't mean all but all on org level
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.organization_id == org_id)
        .filter(CognitionEnvironmentVariable.project_id == None)
        .order_by(CognitionEnvironmentVariable.created_at.asc())
        .all()
    )


def get_all_by_org_env_id(
    org_id: str, env_id: str
) -> List[CognitionEnvironmentVariable]:
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.organization_id == org_id)
        .filter(CognitionEnvironmentVariable.id == env_id)
        .all()
    )


def get_all_in_org(
    org_id: str, only_project_id: Optional[str] = None
) -> List[Dict[str, str]]:
    # collects everything (org and none or specific) from an org and ensures value is hidden
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    base_select_columns = general.construct_select_columns(
        "environment_variable", "cognition", "ev", ["value", "organization_id"], None, 3
    )

    project_filter = ""
    if only_project_id:
        only_project_id = prevent_sql_injection(
            only_project_id, isinstance(only_project_id, str)
        )
        project_filter = (
            f"AND (ev.project_id = '{only_project_id}' OR ev.project_id IS NULL)"
        )
    query = f"""
    SELECT array_agg(row_to_json(x))
    FROM (
        SELECT 
            {base_select_columns},
            CASE WHEN ev.is_secret THEN NULL ELSE ev.value END "value",
            LENGTH(ev.value) "value_length",
            CASE WHEN ev.project_id IS NULL THEN 'ORGANIZATION' ELSE 'PROJECT' END "scope",
            p.name project_name,
            COALESCE(ev.organization_id, p.organization_id) organization_id
        FROM cognition.environment_variable ev
        LEFT JOIN cognition.project p
            ON ev.project_id = p.id AND p.organization_id = '{org_id}'
        WHERE (p.organization_id = '{org_id}' OR ev.organization_id = '{org_id}')
        {project_filter}
    ) x
    """
    result = general.execute_first(query)
    if result and result[0]:
        return result[0]
    return []


def can_access_org_env_var(org_id: str, environment_variable_id: str) -> bool:
    # since org specific env vars dont have a project_id but we still need to check the access rights
    # we collect from the requested env var and match with org id from middleware

    q = session.query(CognitionEnvironmentVariable.organization_id).filter(
        CognitionEnvironmentVariable.id == environment_variable_id,
        CognitionEnvironmentVariable.organization_id == org_id,
    )
    return session.query(q.exists()).scalar()


def create(
    user_id: str,
    name: str,
    description: str,
    value: str,
    is_secret: bool,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    project_id: Optional[str] = None,
    organization_id: Optional[str] = None,
) -> CognitionEnvironmentVariable:
    # project_id filled or org_id filled decides the scope, if both are filled => error

    if project_id is None and organization_id is None:
        raise ValueError("project_id or org_id must be filled")
    if project_id is not None and organization_id is not None:
        raise ValueError("project_id and org_id cannot be filled at the same time")

    environment_variable: CognitionEnvironmentVariable = CognitionEnvironmentVariable(
        created_by=user_id,
        organization_id=organization_id,
        project_id=project_id,
        name=name,
        description=description,
        value=value,
        is_secret=is_secret,
        created_at=created_at,
    )
    general.add(environment_variable, with_commit)
    return environment_variable


def update(
    project_id: str,
    environment_variable_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    value: Optional[str] = None,
    is_secret: Optional[bool] = None,
    with_commit: bool = True,
) -> CognitionEnvironmentVariable:
    environment_variable: CognitionEnvironmentVariable = get(
        project_id, environment_variable_id
    )

    if name is not None:
        environment_variable.name = name
    if description is not None:
        environment_variable.description = description
    if value is not None:
        environment_variable.value = value
    if is_secret is not None:
        environment_variable.is_secret = is_secret
    general.flush_or_commit(with_commit)
    return environment_variable


def delete(
    project_id: str, environment_variable_id: str, with_commit: bool = True
) -> None:
    session.query(CognitionEnvironmentVariable).filter(
        CognitionEnvironmentVariable.project_id == project_id,
        CognitionEnvironmentVariable.id == environment_variable_id,
    ).delete()
    general.flush_or_commit(with_commit)
