from typing import List, Optional
from ..business_objects import general
from ..session import session
from ..models import CognitionEnvironmentVariable


def get(environment_variable_id: str) -> CognitionEnvironmentVariable:
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.id == environment_variable_id)
        .first()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionEnvironmentVariable]:
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.project_id == project_id)
        .order_by(CognitionEnvironmentVariable.created_at.asc())
        .all()
    )


def get_all_by_ids(
    environment_variable_ids: List[str],
) -> List[CognitionEnvironmentVariable]:
    return (
        session.query(CognitionEnvironmentVariable)
        .filter(CognitionEnvironmentVariable.id.in_(environment_variable_ids))
        .order_by(CognitionEnvironmentVariable.created_at.asc())
        .all()
    )


def create(
    user_id: str,
    project_id: str,
    name: str,
    description: str,
    value: str,
    is_secret: bool,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> CognitionEnvironmentVariable:
    environment_variable: CognitionEnvironmentVariable = CognitionEnvironmentVariable(
        created_by=user_id,
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
    environment_variable_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    value: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionEnvironmentVariable:
    environment_variable: CognitionEnvironmentVariable = get(environment_variable_id)
    general.flush_or_commit(with_commit)

    if name is not None:
        environment_variable.name = name
    if description is not None:
        environment_variable.description = description
    if value is not None:
        environment_variable.value = value
    general.flush_or_commit(with_commit)
    return environment_variable


def delete(environment_variable_id: str, with_commit: bool = True) -> None:
    session.query(CognitionEnvironmentVariable).filter(
        CognitionEnvironmentVariable.id == environment_variable_id,
    ).delete()
    general.flush_or_commit(with_commit)
