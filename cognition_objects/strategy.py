from typing import List, Optional
from datetime import datetime

from ..business_objects import general
from ..session import session
from ..models import CognitionStrategy


def get(strategy_id: str) -> CognitionStrategy:
    return (
        session.query(CognitionStrategy)
        .filter(CognitionStrategy.id == strategy_id)
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
        .order_by(CognitionStrategy.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    with_commit: bool = True,
    created_at: Optional[datetime] = None,
) -> CognitionStrategy:
    strategy: CognitionStrategy = CognitionStrategy(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
    )
    general.add(strategy, with_commit)

    return strategy


def update(
    strategy_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    with_commit: bool = True,
) -> CognitionStrategy:
    strategy = get(strategy_id)
    if name is not None:
        strategy.name = name
    if description is not None:
        strategy.description = description
    general.add(strategy, with_commit)

    return strategy


def delete(strategy_id: str, with_commit: bool = True) -> None:
    session.query(CognitionStrategy).filter(
        CognitionStrategy.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)
