from typing import List, Optional
from datetime import datetime
from ..business_objects import general
from ..session import session
from ..models import CognitionEnvironmentVariable


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
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.project_id == project_id)
        .filter(CognitionEnvironmentVariable.name == name)
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


def get_all_by_org_id(org_id: str) -> List[CognitionEnvironmentVariable]:
    # Note, atm this doesn't mean all but all on org level
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.organization_id == org_id)
        .filter(CognitionEnvironmentVariable.project_id == None)
        .order_by(CognitionEnvironmentVariable.created_at.asc())
        .all()
    )


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
