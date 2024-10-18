from typing import List, Optional
from datetime import datetime

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
) -> CognitionStrategy:
    strategy: CognitionStrategy = CognitionStrategy(
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
