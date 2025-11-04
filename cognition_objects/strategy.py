from typing import Any, List, Optional
from datetime import datetime

from submodules.model.util import prevent_sql_injection

from ..business_objects import general
from ..session import session
from ..models import CognitionStrategy
from ..enums import StrategyComplexity


def get(project_id: str, strategy_id: str) -> CognitionStrategy:
    return (
        session.query(CognitionStrategy)
        .filter(
            CognitionStrategy.project_id == project_id,
            CognitionStrategy.id == strategy_id,
        )
        .first()
    )


def get_by_name(project_id: str, name: str) -> CognitionStrategy:
    return (
        session.query(CognitionStrategy)
        .filter(
            CognitionStrategy.project_id == project_id, CognitionStrategy.name == name
        )
        .first()
    )


def get_all_by_project_id(project_id: str) -> List[CognitionStrategy]:
    return (
        session.query(CognitionStrategy)
        .filter(CognitionStrategy.project_id == project_id)
        .order_by(CognitionStrategy.order.desc())
        .order_by(CognitionStrategy.created_at.desc())
        .all()
    )


def get_strategies_without_complexity() -> List[CognitionStrategy]:
    return (
        session.query(CognitionStrategy)
        .filter(CognitionStrategy.complexity == None)
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
    order: Optional[int] = None,
    id: Optional[str] = None,
) -> CognitionStrategy:
    strategy: CognitionStrategy = CognitionStrategy(
        id=id,
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
        order=order,
    )
    general.add(strategy, with_commit)

    return strategy


def update(
    project_id: str,
    strategy_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    complexity: Optional[StrategyComplexity] = None,
    order: Optional[int] = None,
    with_commit: bool = True,
) -> CognitionStrategy:
    strategy = get(project_id, strategy_id)
    if name is not None:
        strategy.name = name
    if description is not None:
        strategy.description = description
    if complexity is not None:
        strategy.complexity = complexity.value
    if order is not None:
        strategy.order = order

    general.add(strategy, with_commit)

    return strategy


def delete(project_id: str, strategy_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategy).filter(
        CognitionStrategy.project_id == project_id,
        CognitionStrategy.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_all_by_project_id(project_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategy).filter(
        CognitionStrategy.project_id == project_id
    ).delete()
    general.flush_or_commit(with_commit)


def get_strategies_info(
    step_types: List[str],
    created_at_from: str,
    created_at_to: Optional[str] = None,
) -> List[Any]:

    step_types = prevent_sql_injection(step_types, isinstance(step_types, list))
    created_at_from = prevent_sql_injection(
        created_at_from, isinstance(created_at_from, str)
    )
    if created_at_to:
        created_at_to = prevent_sql_injection(
            created_at_to, isinstance(created_at_to, str)
        )
    created_at_to_filter = ""

    if created_at_to:
        created_at_to_filter = f"AND ss.created_at <= '{created_at_to}'"

    query = f"""
    SELECT 
        s.id as strategy_id, s.name as strategy_name, 
        ss.id as step_id, ss.created_by, ss.created_at, ss.name as step_name, ss.step_type , 
        p.name as project_name, p.id as project_id,
        o.name as organization_name, o.id as organization_id
    FROM cognition.strategy s 
    JOIN cognition.strategy_step ss on ss.strategy_id = s.id
    JOIN cognition.project p on p.id = s.project_id 
    JOIN organization o on o.id = p.organization_id 
    WHERE ss.created_at >= '{created_at_from}' 
    AND ss.step_type IN ({', '.join(f"'{step_type}'" for step_type in step_types)})
    {created_at_to_filter}
    ORDER BY s.id, ss.created_at DESC
    """

    return general.execute_all(query)
