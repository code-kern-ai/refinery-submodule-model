from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..cognition_objects import message
from ..business_objects import general
from ..session import session
from ..models import Strategy
from .. import enums
from sqlalchemy import func, alias, Integer
from sqlalchemy.orm import aliased


def get(strategy_id: str) -> Strategy:
    return session.query(Strategy).filter(Strategy.id == strategy_id).first()


def get_all_by_project_id(project_id: str) -> List[Strategy]:
    return (
        session.query(Strategy)
        .filter(Strategy.project_id == project_id)
        .order_by(Strategy.created_at.asc())
        .all()
    )


def create(
    project_id: str,
    user_id: str,
    name: str,
    description: str,
    with_commit: bool = True,
    created_at: Optional[str] = None,
) -> Strategy:
    strategy: Strategy = Strategy(
        project_id=project_id,
        created_by=user_id,
        created_at=created_at,
        name=name,
        description=description,
    )
    general.add(strategy, with_commit)

    return strategy


def delete(strategy_id: str, with_commit: bool = True) -> None:
    session.query(Strategy).filter(
        Strategy.id == strategy_id,
    ).delete()
    general.flush_or_commit(with_commit)
